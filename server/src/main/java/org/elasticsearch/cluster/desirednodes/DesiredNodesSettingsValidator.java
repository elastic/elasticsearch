/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.desirednodes;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.DesiredNode;
import org.elasticsearch.core.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.lang.String.format;

public class DesiredNodesSettingsValidator implements Consumer<List<DesiredNode>> {
    private record DesiredNodeValidationError(int position, @Nullable String externalId, RuntimeException exception) {}

    @Override
    public void accept(List<DesiredNode> nodes) {
        final List<DesiredNodeValidationError> validationErrors = new ArrayList<>();
        for (int i = 0; i < nodes.size(); i++) {
            final DesiredNode node = nodes.get(i);
            if (node.version().before(Version.CURRENT)) {
                validationErrors.add(
                    new DesiredNodeValidationError(
                        i,
                        node.externalId(),
                        new IllegalArgumentException(
                            format(
                                Locale.ROOT,
                                "Illegal node version [%s]. Only [%s] or newer versions are supported",
                                node.version(),
                                Build.current().version()
                            )
                        )
                    )
                );
            }
        }

        if (validationErrors.isEmpty() == false) {
            final String nodeIndicesWithFailures = validationErrors.stream()
                .map(DesiredNodeValidationError::position)
                .map(i -> Integer.toString(i))
                .collect(Collectors.joining(","));

            final String nodeIdsWithFailures = validationErrors.stream()
                .map(DesiredNodeValidationError::externalId)
                .collect(Collectors.joining(","));
            IllegalArgumentException invalidSettingsException = new IllegalArgumentException(
                format(
                    Locale.ROOT,
                    "Nodes with ids [%s] in positions [%s] contain invalid settings",
                    nodeIdsWithFailures,
                    nodeIndicesWithFailures
                )
            );
            for (DesiredNodeValidationError exceptionTuple : validationErrors) {
                invalidSettingsException.addSuppressed(exceptionTuple.exception);
            }
            throw invalidSettingsException;
        }
    }

}
