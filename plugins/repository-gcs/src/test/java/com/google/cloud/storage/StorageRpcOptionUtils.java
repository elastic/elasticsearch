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

package com.google.cloud.storage;

import com.google.cloud.storage.spi.v1.StorageRpc;

/**
 * Utility class that exposed Google SDK package protected methods to
 * create specific StorageRpc objects in unit tests.
 */
public class StorageRpcOptionUtils {

    private StorageRpcOptionUtils(){}

    public static String getPrefix(final Storage.BlobListOption... options) {
        if (options != null) {
            for (final Option option : options) {
                final StorageRpc.Option rpcOption = option.getRpcOption();
                if (StorageRpc.Option.PREFIX.equals(rpcOption)) {
                    return (String) option.getValue();
                }
            }
        }
        return null;
    }
}
