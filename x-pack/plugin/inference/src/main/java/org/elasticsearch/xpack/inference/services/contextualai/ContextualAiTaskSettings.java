/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.contextualai;

import org.elasticsearch.inference.TaskSettings;

/**
 * Base class for TaskSettings of Contextual AI inference tasks.
 * Exists to group together shared logic and constants between different Contextual AI tasks.
 */
public abstract class ContextualAiTaskSettings implements TaskSettings {}
