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

package org.elasticsearch.index.engine;

import org.elasticsearch.index.translog.Translog;

/** Holds a deleted version, which just adds a timestamp to {@link VersionValue} so we know when we can expire the deletion. */

class DeleteVersionValue extends VersionValue {
    private final long time;

    public DeleteVersionValue(long version, long time, Translog.Location translogLocation) {
        super(version, translogLocation);
        this.time = time;
    }

    @Override
    public long time() {
        return this.time;
    }

    @Override
    public boolean delete() {
        return true;
    }

    @Override
    public long ramBytesUsed() {
        return super.ramBytesUsed() + Long.BYTES;
    }
}
