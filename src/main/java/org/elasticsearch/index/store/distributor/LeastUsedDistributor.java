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

package org.elasticsearch.index.store.distributor;

import jsr166y.ThreadLocalRandom;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.store.DirectoryService;

import java.io.IOException;

/**
 * Implements directory distributor that always return the directory is the most available space
 */
public class LeastUsedDistributor extends AbstractDistributor {

    @Inject
    public LeastUsedDistributor(DirectoryService directoryService) throws IOException {
        super(directoryService);
    }

    @Override
    public Directory doAny() {
        Directory directory = null;
        long size = Long.MIN_VALUE;
        int sameSize = 0;
        for (Directory delegate : delegates) {
            long currentSize = getUsableSpace(delegate);
            if (currentSize > size) {
                size = currentSize;
                directory = delegate;
                sameSize = 1;
            } else if (currentSize == size) {
                sameSize++;
                // Ensure uniform distribution between all directories with the same size
                if (ThreadLocalRandom.current().nextDouble() < 1.0 / sameSize) {
                    directory = delegate;
                }
            }
        }

        return directory;
    }

    @Override
    public String name() {
        return "least_used";
    }

}
