/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datageneration;

import java.util.Map;

/**
 * Contains generated mapping and supporting data.
 * @param raw mapping represented as a possibly nested map (maps represent (sub-)objects)
 * @param lookup supporting data structure that represent mapping in a flat form (full path to field -> mapping parameters)
 */
public record Mapping(Map<String, Object> raw, Map<String, Map<String, Object>> lookup) {}
