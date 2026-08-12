/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

/// When recovery fails we call [RecoveryListener#onRecoveryFailure] with a [FailureStrategy].
/// The failure strategy is selected on the call-site and filters through this selector
/// which makes it possible to select a different failure strategy based on the exception.
/// Very useful for testing. Injectable via [org.elasticsearch.plugins.RecoveryFailureStrategySelectorPlugin]
public interface FailureStrategySelector {
    FailureStrategySelector DEFAULT = (exception, defaultStrategy) -> defaultStrategy;

    FailureStrategy select(Exception exception, FailureStrategy defaultStrategy);
}
