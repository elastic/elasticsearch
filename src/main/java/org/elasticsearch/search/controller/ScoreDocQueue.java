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

package org.elasticsearch.search.controller;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.util.PriorityQueue;

/**
 * <p>Same as lucene {@link org.apache.lucene.search.HitQueue}.
 */
public class ScoreDocQueue extends PriorityQueue<ScoreDoc> {

    public ScoreDocQueue(int size) {
        super(size);
    }

    protected final boolean lessThan(ScoreDoc hitA, ScoreDoc hitB) {
        if (hitA.score == hitB.score) {
            int c = hitA.shardIndex - hitB.shardIndex;
            if (c == 0) {
                return hitA.doc > hitB.doc;
            }
            return c > 0;
        } else {
            return hitA.score < hitB.score;
        }
    }
}
