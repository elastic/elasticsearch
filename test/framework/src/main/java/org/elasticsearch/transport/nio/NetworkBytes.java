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

package org.elasticsearch.transport.nio;

import org.elasticsearch.common.lease.Releasable;

public interface NetworkBytes extends Releasable {

    int getWriteIndex();

    void incrementWrite(int delta);

    int getWriteRemaining();

    boolean hasWriteRemaining();

    int getReadIndex();

    void incrementRead(int delta);

    int getReadRemaining();

    boolean hasReadRemaining() ;

    static void validateReadIndex(int newReadIndex, int writeIndex) {
        if (newReadIndex > writeIndex) {
            throw new IndexOutOfBoundsException("New read index [" + newReadIndex + "] would be greater than write" +
                " index [" + writeIndex + "]");
        }
    }

    static void validateWriteIndex(int newWriteIndex, int length) {
        if (newWriteIndex > length) {
            throw new IndexOutOfBoundsException("New write index [" + newWriteIndex + "] would be greater than length" +
                " [" + length + "]");
        }
    }
}
