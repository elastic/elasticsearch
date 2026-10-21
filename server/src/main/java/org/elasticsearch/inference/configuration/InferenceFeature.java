/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.configuration;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.xcontent.ToXContentObject;

/**
 * A single capability an inference service reports through {@code GET _inference/_services}.
 * <p>
 * Implementations are resolved polymorphically by {@link #getWriteableName()} on both the XContent and transport
 * layers, so each feature owns its own serialized shape and services declare only the features that apply to them.
 * Adding a new feature means writing the implementation and registering it in {@link InferenceServiceFeatures}.
 */
public interface InferenceFeature extends ToXContentObject, NamedWriteable {}
