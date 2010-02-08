/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.util.testng;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.LoggerFactory;
import org.testng.ITestContext;
import org.testng.ITestResult;
import org.testng.TestListenerAdapter;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

/**
 * @author kimchy (Shay Banon)
 */
public class LoggingListener extends TestListenerAdapter {

    @Override public void onStart(ITestContext context) {
        String logsDir = context.getOutputDirectory() + "/logs";
        deleteRecursively(new File(logsDir), false);
        System.setProperty("test.log.dir", logsDir);
    }

    @Override public void onTestStart(ITestResult result) {
        String logName = result.getTestClass().getName();
        if (logName.startsWith("org.elasticsearch.")) {
            logName = logName.substring("org.elasticsearch.".length());
        }
        System.setProperty("test.log.name", logName);

        Properties props = new Properties();
        try {
            props.load(LoggingListener.class.getClassLoader().getResourceAsStream(System.getProperty("es.test.log.conf", "log4j.properties")));
        } catch (IOException e) {
            e.printStackTrace();
        }
        PropertyConfigurator.configure(props);

        LoggerFactory.getLogger("testng").info("========== Starting Test [" + result.getName() + "] ==========");
    }

    @Override public void onTestSuccess(ITestResult result) {
        LoggerFactory.getLogger("testng").info("========== Test Success [" + result.getName() + "] ==========");
    }

    @Override public void onTestFailure(ITestResult result) {
        LoggerFactory.getLogger("testng").info("========== Test Failure [" + result.getName() + "] ==========");
    }

    @Override public void onTestSkipped(ITestResult result) {
        LoggerFactory.getLogger("testng").info("========== Test Skipped [" + result.getName() + "] ==========");
    }

    /**
     * Delete the supplied {@link java.io.File} - for directories,
     * recursively delete any nested directories or files as well.
     *
     * @param root       the root <code>File</code> to delete
     * @param deleteRoot whether or not to delete the root itself or just the content of the root.
     * @return <code>true</code> if the <code>File</code> was deleted,
     *         otherwise <code>false</code>
     */
    public static boolean deleteRecursively(File root, boolean deleteRoot) {
        if (root != null && root.exists()) {
            if (root.isDirectory()) {
                File[] children = root.listFiles();
                if (children != null) {
                    for (File aChildren : children) {
                        deleteRecursively(aChildren, true);
                    }
                }
            }

            if (deleteRoot) {
                return root.delete();
            } else {
                return true;
            }
        }
        return false;
    }
}