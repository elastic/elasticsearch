/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.validation;

import org.elasticsearch.inference.Model;

/**
 * Holds the result of model validation, including the validated model and whether the validator started
 * a model deployment as part of validation.
 */
public record ModelValidationResult(Model model, boolean deploymentStarted) {}
