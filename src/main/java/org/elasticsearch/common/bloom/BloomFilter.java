/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.bloom;

/**
 *
 */
public interface BloomFilter {

    public static final BloomFilter NONE = new BloomFilter() {
        @Override
        public void add(byte[] key, int offset, int length) {
        }

        @Override
        public boolean isPresent(byte[] key, int offset, int length) {
            return true;
        }

        @Override
        public long sizeInBytes() {
            return 0;
        }
    };

    public static final BloomFilter EMPTY = new BloomFilter() {
        @Override
        public void add(byte[] key, int offset, int length) {
        }

        @Override
        public boolean isPresent(byte[] key, int offset, int length) {
            return false;
        }

        @Override
        public long sizeInBytes() {
            return 0;
        }
    };

    void add(byte[] key, int offset, int length);

    boolean isPresent(byte[] key, int offset, int length);

    long sizeInBytes();
}