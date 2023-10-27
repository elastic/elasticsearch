/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.inference.services.elser.ElserMlNodeServiceSettings;
import org.elasticsearch.xpack.inference.services.elser.ElserMlNodeTaskSettings;
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserSecretSettings;
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserServiceSettings;

import java.util.ArrayList;
import java.util.List;

public class InferenceNamedWriteablesProvider {

    private InferenceNamedWriteablesProvider() {}

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();

        // Empty default settings
        namedWriteables.add(new NamedWriteableRegistry.Entry(EmptyTaskSettings.class, EmptyTaskSettings.NAME, EmptyTaskSettings::new));

        // ELSER config
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(ServiceSettings.class, ElserMlNodeServiceSettings.NAME, ElserMlNodeServiceSettings::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(TaskSettings.class, ElserMlNodeTaskSettings.NAME, ElserMlNodeTaskSettings::new)
        );

        // Hugging Face ELSER config
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                HuggingFaceElserServiceSettings.NAME,
                HuggingFaceElserServiceSettings::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(SecretSettings.class, HuggingFaceElserSecretSettings.NAME, HuggingFaceElserSecretSettings::new)
        );

        return namedWriteables;
    }
}
