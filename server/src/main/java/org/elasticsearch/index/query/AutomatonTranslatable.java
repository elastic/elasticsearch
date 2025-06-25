/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.index.query;

import org.apache.lucene.util.automaton.Automaton;

/**
 * Query that matches documents based on a Lucene Automaton.
 */
public interface AutomatonTranslatable {
    Automaton getAutomaton();

    String getAutomatonDescription();
}
