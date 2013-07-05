/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.indices;

import java.util.Locale;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.rest.RestStatus;

/**
 */
public class AliasMissingException extends ElasticSearchException {

    private final String[] names;

    public AliasMissingException(String... names) {
        super(String.format(Locale.ROOT, "alias [%s] missing", toNamesString(names)));
        this.names = names;
    }

    public String[] getName() {
        return names;
    }

    @Override
    public RestStatus status() {
        return RestStatus.NOT_FOUND;
    }

    private static String toNamesString(String... names) {
        if (names == null || names.length == 0) {
            return "";
        } else if (names.length == 1) {
            return names[0];
        } else {
            StringBuilder builder = new StringBuilder(names[0]);
            for (int i = 1; i < names.length; i++) {
                builder.append(',').append(names[i]);
            }
            return builder.toString();
        }
    }
}
