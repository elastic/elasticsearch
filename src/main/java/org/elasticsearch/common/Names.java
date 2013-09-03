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

package org.elasticsearch.common;

import com.google.common.base.Charsets;
import jsr166y.ThreadLocalRandom;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Random;

/**
 *
 */
public abstract class Names {

    public static String randomNodeName(URL nodeNames) {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(nodeNames.openStream(), Charsets.UTF_8));
            int numberOfNames = 0;
            while (reader.readLine() != null) {
                numberOfNames++;
            }
            reader.close();
            reader = new BufferedReader(new InputStreamReader(nodeNames.openStream(), Charsets.UTF_8));
            int number = ((ThreadLocalRandom.current().nextInt(numberOfNames)) % numberOfNames);
            for (int i = 0; i < number; i++) {
                reader.readLine();
            }
            return reader.readLine();
        } catch (IOException e) {
            return null;
        } finally {
            try {
                if (reader != null) {
                    reader.close();
                }
            } catch (IOException e) {
                // ignore this exception
            }
        }
    }

    public static String randomNodeName(InputStream nodeNames) {
        if (nodeNames == null) {
            return null;
        }
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(nodeNames, Charsets.UTF_8));
            int numberOfNames = Integer.parseInt(reader.readLine());
            int number = ((new Random().nextInt(numberOfNames)) % numberOfNames) - 2; // remove 2 for last line and first line
            for (int i = 0; i < number; i++) {
                reader.readLine();
            }
            return reader.readLine();
        } catch (Exception e) {
            return null;
        } finally {
            try {
                nodeNames.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    private Names() {

    }
}
