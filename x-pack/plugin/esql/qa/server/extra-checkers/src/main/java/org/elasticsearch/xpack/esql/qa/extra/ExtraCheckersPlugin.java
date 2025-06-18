/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.extra;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.esql.analysis.Verifier;

/**
 * Marker plugin to enable {@link Verifier.ExtraCheckers}.
 */
public class ExtraCheckersPlugin extends Plugin {}
