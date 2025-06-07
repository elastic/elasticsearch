/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import java.util.List;

/**
 * Track failures that occur during the resolution phase on the coordinator. These will be reported in the final response.
 */
public record ResolutionFailure(List<Exception> failures) {

}
