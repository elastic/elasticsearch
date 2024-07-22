/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan;

import org.elasticsearch.xpack.esql.core.expression.Attribute;

import java.util.List;

public interface GeneratingPlan<PlanType extends GeneratingPlan<PlanType>> {
    List<Attribute> generatedAttributes();

    /**
     * Create a new instance of this node with new output {@link Attribute}s using the given names.
     * The output attributes have new {@link org.elasticsearch.xpack.esql.core.expression.NameId}s.
     */
    // TODO: the generated attributes should probably become synthetic once renamed
    // blocked on https://github.com/elastic/elasticsearch/issues/98703
    PlanType withGeneratedNames(List<String> newNames);
}
