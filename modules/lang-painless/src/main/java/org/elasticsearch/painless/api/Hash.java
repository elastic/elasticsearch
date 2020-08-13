/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.elasticsearch.painless.api;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Hash {

    public static String hash(String input, String method) throws NoSuchAlgorithmException {
        // Accepts all message digest algorithms supported by
        // java.security.MessageDigest
        MessageDigest md = MessageDigest.getInstance(method);
        byte[] messageDigest = md.digest(input.getBytes());
        BigInteger mdNum = new BigInteger(1, messageDigest);

        // Convert message digest into hex value
        String hashString = mdNum.toString(16);
        while (hashString.length() < 32) {
            hashString = "0" + hashString;
        }
        return hashString;
    }
}
