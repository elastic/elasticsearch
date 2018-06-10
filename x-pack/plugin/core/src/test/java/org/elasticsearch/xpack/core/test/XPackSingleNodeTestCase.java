/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.test;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import org.elasticsearch.test.ESSingleNodeTestCase;

@ThreadLeakFilters(filters = {ObjectCleanerThreadThreadFilter.class})
public abstract class XPackSingleNodeTestCase extends ESSingleNodeTestCase {
}
