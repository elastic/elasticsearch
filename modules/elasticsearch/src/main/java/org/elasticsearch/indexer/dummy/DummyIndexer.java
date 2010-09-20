/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.indexer.dummy;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.indexer.AbstractIndexerComponent;
import org.elasticsearch.indexer.Indexer;
import org.elasticsearch.indexer.IndexerName;
import org.elasticsearch.indexer.IndexerSettings;

/**
 * @author kimchy (shay.banon)
 */
public class DummyIndexer extends AbstractIndexerComponent implements Indexer {

    @Inject public DummyIndexer(IndexerName indexerName, IndexerSettings settings) {
        super(indexerName, settings);
        logger.info("create");
    }

    @Override public void start() {
        logger.info("start");
    }

    @Override public void close() {
        logger.info("close");
    }
}
