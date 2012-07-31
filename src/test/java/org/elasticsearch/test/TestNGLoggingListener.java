/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.test;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;

/**
 */
public class TestNGLoggingListener implements ITestListener {

    private ESLogger logger = ESLoggerFactory.getLogger("test");

    private String extractTestName(ITestResult result) {
        String testName = result.getInstanceName();
        if (testName.startsWith("org.elasticsearch.")) {
            testName = testName.substring("org.elasticsearch.".length());
        }
        if (testName.startsWith("test.")) {
            testName = testName.substring("test.".length());
        }
        return testName + "#" + result.getName();
    }

    @Override
    public void onTestStart(ITestResult result) {
        logger.info("==> Test Starting [{}]", extractTestName(result));
    }

    @Override
    public void onTestSuccess(ITestResult result) {
        logger.info("==> Test Success [{}]", extractTestName(result));
    }

    @Override
    public void onTestFailure(ITestResult result) {
        logger.error("==> Test Failure [{}]", extractTestName(result));
    }

    @Override
    public void onTestSkipped(ITestResult result) {
        logger.info("==> Test Skipped [{}]", extractTestName(result));
    }

    @Override
    public void onTestFailedButWithinSuccessPercentage(ITestResult result) {
    }

    @Override
    public void onStart(ITestContext context) {
    }

    @Override
    public void onFinish(ITestContext context) {
    }
}
