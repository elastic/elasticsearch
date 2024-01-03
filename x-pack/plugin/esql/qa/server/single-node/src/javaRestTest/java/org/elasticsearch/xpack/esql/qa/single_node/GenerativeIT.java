/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import org.apache.lucene.tests.util.LuceneTestCase.AwaitsFix;
import org.elasticsearch.xpack.esql.qa.rest.generative.GenerativeRestTest;

@AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/102084")
public class GenerativeIT extends GenerativeRestTest {}
