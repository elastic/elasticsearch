/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * Infrastructure to run suites of tests written in YAML against a running Elasticsearch cluster using Elasticsearch's low level REST
 * client. The YAML tests are run by all official clients and serve as tests for both Elasticsearch and the clients.
 */
package org.elasticsearch.test.rest.yaml;
