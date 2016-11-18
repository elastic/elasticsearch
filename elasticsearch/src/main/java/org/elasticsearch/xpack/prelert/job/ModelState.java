/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job;


/**
 * The serialised models can get very large and only the C++ code
 * understands how to decode them, hence there is no reason to load
 * them into the Java process.
 *
 * However, the Java process DOES set up a mapping on the Elasticsearch
 * index to tell Elasticsearch not to analyse the model state documents
 * in any way.  (Otherwise Elasticsearch would go into a spin trying to
 * make sense of such large JSON documents.)
 */
public class ModelState
{
    /**
     * The type of this class used when persisting the data
     */
    public static final String TYPE = "modelState";

    private ModelState()
    {
    }
}

