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

package org.elasticsearch;

import org.elasticsearch.monitor.jvm.JvmInfo;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.TimeZone;

/**
 * @author kimchy (shay.banon)
 */
public class Version {

    private static final String number;
    private static final String date;
    private static final boolean snapshotBuild;


    static {
        Properties props = new Properties();
        try {
            InputStream stream = Version.class.getClassLoader().getResourceAsStream("org/elasticsearch/version.properties");
            props.load(stream);
            stream.close();
        } catch (Exception e) {
            // ignore
        }

        number = props.getProperty("number", "0.0.0");
        snapshotBuild = number.contains("-SNAPSHOT");
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        date = props.getProperty("date", sdf.format(new Date()));
    }

    public static String number() {
        return number;
    }

    public static String date() {
        return date;
    }

    public static boolean snapshotBuild() {
        return snapshotBuild;
    }

    public static String full() {
        StringBuilder sb = new StringBuilder("ElasticSearch/");
        sb.append(number);
        if (snapshotBuild) {
            sb.append("/").append(date);
        }
        return sb.toString();
    }

    public static void main(String[] args) {
        System.out.println("ElasticSearch Version: " + number + " (" + date() + "), JVM: " + JvmInfo.jvmInfo().vmVersion());
    }
}
