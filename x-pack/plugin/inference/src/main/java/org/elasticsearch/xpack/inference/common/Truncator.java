/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Provides truncation logic for inference requests
 */
public class Truncator {

    /**
     * Defines the percentage to reduce the input text for an inference request.
     */
    static final Setting<Double> REDUCTION_PERCENTAGE_SETTING = Setting.doubleSetting(
        "xpack.inference.truncator.reduction_percentage",
        0.5,
        0.01,
        0.99,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static List<Setting<?>> getSettingsDefinitions() {
        return List.of(REDUCTION_PERCENTAGE_SETTING);
    }

    /**
     * OpenAI estimates that there are 4 characters per token
     * <a href="https://help.openai.com/en/articles/4936856-what-are-tokens-and-how-to-count-them">here</a>.
     * We'll take a conservative approach and assume there's a token every 3 characters.
     */
    private static final double CHARS_PER_TOKEN = 3;

    public static double countTokens(String text) {
        return Math.ceil(text.length() / CHARS_PER_TOKEN);
    }

    private volatile double reductionPercentage;

    public Truncator(Settings settings, ClusterService clusterService) {
        this.reductionPercentage = REDUCTION_PERCENTAGE_SETTING.get(settings);

        clusterService.getClusterSettings().addSettingsUpdateConsumer(REDUCTION_PERCENTAGE_SETTING, this::setReductionPercentage);
    }

    private void setReductionPercentage(double percentage) {
        reductionPercentage = percentage;
    }

    /**
     * Truncate each entry in the list to the specified number of tokens.
     * @param input list of strings
     * @param tokenLimit the number of tokens to limit the text to
     * @return the resulting list of text and whether it was truncated
     */
    public static TruncationResult truncate(List<String> input, @Nullable Integer tokenLimit) {
        if (tokenLimit == null) {
            return new TruncationResult(input, new boolean[input.size()]);
        }

        var maxLength = maxLength(tokenLimit);

        var truncatedText = new ArrayList<String>(input.size());
        var wasTruncated = new boolean[input.size()];

        for (int i = 0; i < input.size(); i++) {
            var text = input.get(i);
            var truncateResult = truncate(text, maxLength);
            truncatedText.add(truncateResult.input);
            wasTruncated[i] = truncateResult.truncated;
        }

        return new TruncationResult(truncatedText, wasTruncated);
    }

    private static int maxLength(Integer maxTokens) {
        if (maxTokens == null) {
            return Integer.MAX_VALUE;
        }

        return (int) Math.floor(maxTokens * CHARS_PER_TOKEN);
    }

    private static TruncationEntry truncate(String text, int textLength) {
        var truncatedText = text.substring(0, Math.min(text.length(), textLength));
        var truncated = truncatedText.length() < text.length();

        return new TruncationEntry(truncatedText, truncated);
    }

    /**
     * Truncate each entry in the list by the percentage value specified in the {@link #REDUCTION_PERCENTAGE_SETTING} setting.
     * @param input list of strings
     * @return the resulting list of text and whether it was truncated
     */
    public TruncationResult truncate(List<String> input) {
        var truncatedText = new ArrayList<String>(input.size());
        var wasTruncated = new boolean[input.size()];

        for (int i = 0; i < input.size(); i++) {
            var text = input.get(i);
            var truncateResult = truncate(text);
            truncatedText.add(truncateResult.input);
            wasTruncated[i] = truncateResult.truncated;
        }

        return new TruncationResult(truncatedText, wasTruncated);
    }

    private TruncationEntry truncate(String text) {
        var length = (int) Math.floor(text.length() * reductionPercentage);
        return truncate(text, length);
    }

    private record TruncationEntry(String input, boolean truncated) {}

    public record TruncationResult(List<String> input, boolean[] truncated) {

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TruncationResult that = (TruncationResult) o;
            return Objects.equals(input, that.input) && Arrays.equals(truncated, that.truncated);
        }

        @Override
        public int hashCode() {
            return Objects.hash(input, Arrays.hashCode(truncated));
        }
    }
}
