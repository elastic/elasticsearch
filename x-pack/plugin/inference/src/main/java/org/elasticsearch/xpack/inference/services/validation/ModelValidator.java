/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.validation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.inference.InferenceService;
import org.elasticsearch.inference.Model;

public interface ModelValidator {
    void validate(InferenceService service, Model model, ActionListener<Model> listener);
}
