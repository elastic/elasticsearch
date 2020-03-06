/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference.search;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.search.SearchExtBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.utils.MapHelper;
import org.elasticsearch.xpack.ml.inference.loadingservice.Model;
import org.elasticsearch.xpack.ml.inference.loadingservice.ModelLoadingService;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * A very rough sketch of a fetch sub phase that performs inference on each search hit,
 * then augments the hit with the result.
 */
public class InferencePhase implements FetchSubPhase {

    private final SetOnce<ModelLoadingService> modelLoadingService;

    public InferencePhase(SetOnce<ModelLoadingService> modelLoadingService) {
        this.modelLoadingService = modelLoadingService;
    }

    @Override
    public void hitsExecute(SearchContext searchContext, SearchHit[] hits) {
        SearchExtBuilder inferenceBuilder = searchContext.getSearchExt(InferenceSearchExtBuilder.NAME);
        if (inferenceBuilder == null) {
            return;
        }

        InferenceSearchExtBuilder infBuilder = (InferenceSearchExtBuilder)inferenceBuilder;

        SetOnce<Model> model = new SetOnce<>();
        CountDownLatch latch = new CountDownLatch(1);
        ActionListener<Model> listener = new LatchedActionListener<>(
                ActionListener.wrap(model::set, e -> { throw new RuntimeException();}), latch);

        modelLoadingService.get().getModel(infBuilder.getModelId(), listener);
        try {
            // Eeek blocking on a latch we can't be doing that
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        Map<String, String> fieldMap = infBuilder.getFieldMap();
        Set<String> fieldsToRead = new HashSet<>(model.get().getFieldNames());

        // fieldMap is fieldname in doc -> fieldname expected by model.
        // Reverse the map to find the name of the doc field
        for (Map.Entry<String, String> entry : fieldMap.entrySet()) {
            if (fieldsToRead.contains(entry.getValue())) {
                fieldsToRead.remove(entry.getValue());
                fieldsToRead.add(entry.getKey());
            }
        }

        InferenceConfig config = infBuilder.getInferenceConfig();
        for (SearchHit hit : hits) {
            InferenceResults infer = model.get().infer(
                    mapFields(
                            extractFields(hit, fieldsToRead), fieldMap), config);

            addFieldsToHit(hit, infer.writeResultToMap(infBuilder.getTargetField()));
        }
    }

    private Map<String, Object> extractFields(SearchHit hit, Set<String> fieldNames) {

        Map<String, Object> doc = new HashMap<>();
        Set<String> trackingFieldNames = new HashSet<>(fieldNames);

        for (String fieldName : fieldNames) {
            DocumentField field = hit.field(fieldName);
            if (field != null) {
                doc.put(fieldName, field.getValue());
                trackingFieldNames.remove(fieldName);
            }
        }

        Map<String, Object> sourceMap = hit.getSourceAsMap();
        if (sourceMap != null) {
            for (String fieldName : trackingFieldNames) {
                Object fieldValue = MapHelper.dig(fieldName, sourceMap);
                if (fieldValue != null) {
                     doc.put(fieldName, fieldValue);
                }
            }
        }

        return doc;
    }

    private Map<String, Object> mapFields(Map<String, Object> doc, Map<String, String> fieldMap) {
        fieldMap.forEach((src, dest) -> {
            Object srcValue = doc.get(src);
            doc.put(dest, srcValue);
        });

        return doc;
    }

    private void addFieldsToHit(SearchHit hit, Map<String, Object> appends) {
        Map<String, DocumentField> fields = new HashMap<>(hit.getFields());
        for (Map.Entry<String, Object> entry : appends.entrySet()) {
            DocumentField df = new DocumentField(entry.getKey(), Collections.singletonList(entry.getValue()));
            fields.put(df.getName(), df);
        }
        hit.fields(fields);
    }
}
