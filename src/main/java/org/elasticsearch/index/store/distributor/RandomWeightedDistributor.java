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
 * Implements directory distributor that picks a directory at random. The probability of selecting a directory
 * is proportional to the amount of usable space in this directory.
 */
public class RandomWeightedDistributor extends AbstractDistributor {

    @Inject
    public RandomWeightedDistributor(DirectoryService directoryService) throws IOException {
        super(directoryService);
    }

    @Override
    public Directory doAny() {
        long[] usableSpace = new long[delegates.length];
        long size = 0;

        for (int i = 0; i < delegates.length; i++) {
            size += getUsableSpace(delegates[i]);
            usableSpace[i] = size;
        }

        if (size != 0) {
            long random = ThreadLocalRandom.current().nextLong(size);
            for (int i = 0; i < delegates.length; i++) {
                if (usableSpace[i] > random) {
                    return delegates[i];
                }
            }
        }

        // TODO: size is 0 - should we bail out or fall back on random distribution?
        return delegates[ThreadLocalRandom.current().nextInt(delegates.length)];
    }

    @Override
    public String name() {
        return "random";
    }

}
