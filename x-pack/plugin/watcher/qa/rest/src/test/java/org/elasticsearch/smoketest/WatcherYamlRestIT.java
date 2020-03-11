/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.smoketest;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.xpack.watcher.WatcherYamlSuiteTestCase;

/**
 * Runs the YAML rest tests against an external cluster
 */
@LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/53177")
public class WatcherYamlRestIT extends WatcherYamlSuiteTestCase {
    public WatcherYamlRestIT(ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }
}
