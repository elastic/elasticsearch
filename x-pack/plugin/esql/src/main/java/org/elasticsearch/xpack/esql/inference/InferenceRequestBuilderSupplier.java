/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.inference;

import org.elasticsearch.xpack.core.inference.action.InferenceAction;

import java.util.function.Supplier;

public interface InferenceRequestBuilderSupplier extends Supplier<InferenceAction.Request.Builder> {}
