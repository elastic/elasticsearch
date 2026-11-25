/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.coordination;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The join attributes this node sends with each join request, computed per request, and the validators the elected master runs with the
 * attributes of each node it adds.
 */
public record JoinAttributes(Supplier<Map<String, String>> localAttributes, Collection<JoinAttributesValidator> validators) {

    public static final JoinAttributes NONE = new JoinAttributes(Map::of, List.of());
}
