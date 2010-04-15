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

package org.elasticsearch.rest.support;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Map;

/**
 * @author kimchy (shay.banon)
 */
public class RestUtils {

    public static void decodeQueryString(String queryString, int fromIndex, Map<String, String> params) {
        if (fromIndex < 0) {
            return;
        }
        if (fromIndex >= queryString.length()) {
            return;
        }
        int toIndex;
        while ((toIndex = queryString.indexOf('&', fromIndex)) >= 0) {
            int idx = queryString.indexOf('=', fromIndex);
            if (idx < 0) {
                continue;
            }
            params.put(decodeComponent(queryString.substring(fromIndex, idx)), decodeComponent(queryString.substring(idx + 1, toIndex)));
            fromIndex = toIndex + 1;
        }
        int idx = queryString.indexOf('=', fromIndex);
        if (idx < 0) {
            return;
        }
        params.put(decodeComponent(queryString.substring(fromIndex, idx)), decodeComponent(queryString.substring(idx + 1)));
    }

    private static String decodeComponent(String s) {
        if (s == null) {
            return "";
        }
        try {
            return URLDecoder.decode(s, "UTF8");
        } catch (UnsupportedEncodingException e) {
            throw new UnsupportedCharsetException("UTF8");
        }
    }
}
