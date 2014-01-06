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

package org.elasticsearch.indices.store;

import org.apache.lucene.store.Directory;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.store.DirectoryService;
import org.elasticsearch.index.store.distributor.AbstractDistributor;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

/**
 *
 */
public class StrictDistributor extends AbstractDistributor {

    @Inject
    public StrictDistributor(DirectoryService directoryService) throws IOException {
        super(directoryService);
    }

    @Override
    public Directory doAny() {
        for (Directory delegate : delegates) {
            assertThat(getUsableSpace(delegate), greaterThan(0L));
        }
        return primary();
    }

    @Override
    public String name() {
        return "strict";
    }

}