/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.exporter;

import org.elasticsearch.common.component.LifecycleComponent;

import java.util.Collection;

public interface Exporter<T> extends LifecycleComponent<T> {

    String name();

    void export(Collection<MarvelDoc> marvelDocs);
}