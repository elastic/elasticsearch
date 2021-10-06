/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

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

package org.elasticsearch.common.lucene;

import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public abstract class FilterIndexCommit extends IndexCommit {
    protected final IndexCommit in;

    public FilterIndexCommit(IndexCommit in) {
        this.in = in;
    }

    public IndexCommit getIndexCommit() {
        return in;
    }

    @Override
    public String getSegmentsFileName() {
        return in.getSegmentsFileName();
    }

    @Override
    public Collection<String> getFileNames() throws IOException {
        return in.getFileNames();
    }

    @Override
    public Directory getDirectory() {
        return in.getDirectory();
    }

    @Override
    public void delete() {
        in.delete();
    }

    @Override
    public boolean isDeleted() {
        return in.isDeleted();
    }

    @Override
    public int getSegmentCount() {
        return in.getSegmentCount();
    }

    @Override
    public long getGeneration() {
        return in.getGeneration();
    }

    @Override
    public Map<String, String> getUserData() throws IOException {
        return in.getUserData();
    }

    @Override
    public String toString() {
        return "FilterIndexCommit{" + "in=" + in + '}';
    }
}
