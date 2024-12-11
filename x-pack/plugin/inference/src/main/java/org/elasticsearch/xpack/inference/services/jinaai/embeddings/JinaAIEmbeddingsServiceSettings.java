/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

 package org.elasticsearch.xpack.inference.services.jinaai.embeddings;

 import org.elasticsearch.TransportVersion;
 import org.elasticsearch.TransportVersions;
 import org.elasticsearch.common.ValidationException;
 import org.elasticsearch.common.io.stream.StreamInput;
 import org.elasticsearch.common.io.stream.StreamOutput;
 import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
 import org.elasticsearch.inference.ModelConfigurations;
 import org.elasticsearch.inference.ServiceSettings;
 import org.elasticsearch.inference.SimilarityMeasure;
 import org.elasticsearch.xcontent.XContentBuilder;
 import org.elasticsearch.xpack.inference.services.ConfigurationParseContext;
 import org.elasticsearch.xpack.inference.services.ServiceUtils;
 import org.elasticsearch.xpack.inference.services.jinaai.JinaAIServiceSettings;
 import org.elasticsearch.xpack.inference.services.settings.FilteredXContentObject;
 
 import java.io.IOException;
 import java.util.EnumSet;
 import java.util.Locale;
 import java.util.Map;
 import java.util.Objects;
 
 import static org.elasticsearch.xpack.inference.services.ServiceUtils.extractOptionalEnum;
 
 public class JinaAIEmbeddingsServiceSettings extends FilteredXContentObject implements ServiceSettings {
     public static final String NAME = "jinaai_embeddings_service_settings";
  
     public static JinaAIEmbeddingsServiceSettings fromMap(Map<String, Object> map, ConfigurationParseContext context) {
         ValidationException validationException = new ValidationException();
         var commonServiceSettings = JinaAIServiceSettings.fromMap(map, context);
  
         if (validationException.validationErrors().isEmpty() == false) {
             throw validationException;
         }
 
         return new JinaAIEmbeddingsServiceSettings(commonServiceSettings);
     }

 
     private final JinaAIServiceSettings commonSettings;
 
     public JinaAIEmbeddingsServiceSettings(JinaAIServiceSettings commonSettings) {
         this.commonSettings = commonSettings;
     }
 
     public JinaAIEmbeddingsServiceSettings(StreamInput in) throws IOException {
         commonSettings = new JinaAIServiceSettings(in);
     }
 
     public JinaAIServiceSettings getCommonSettings() {
         return commonSettings;
     }
 
     @Override
     public SimilarityMeasure similarity() {
         return commonSettings.similarity();
     }
 
     @Override
     public Integer dimensions() {
         return commonSettings.dimensions();
     }
 
     @Override
     public String modelId() {
         return commonSettings.modelId();
     }
 
     @Override
     public String getWriteableName() {
         return NAME;
     }
 
     @Override
     public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
         builder.startObject();
 
         commonSettings.toXContentFragment(builder, params);
 
         builder.endObject();
         return builder;
     }
 
     @Override
     protected XContentBuilder toXContentFragmentOfExposedFields(XContentBuilder builder, Params params) throws IOException {
         commonSettings.toXContentFragmentOfExposedFields(builder, params);
 
         return builder;
     }
 
     @Override
     public TransportVersion getMinimalSupportedVersion() {
         return TransportVersions.V_8_13_0;
     }
 
     @Override
     public void writeTo(StreamOutput out) throws IOException {
         commonSettings.writeTo(out);
     }
 
     @Override
     public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         JinaAIEmbeddingsServiceSettings that = (JinaAIEmbeddingsServiceSettings) o;
         return Objects.equals(commonSettings, that.commonSettings);
     }
 
     @Override
     public int hashCode() {
         return Objects.hash(commonSettings);
     }
 }
 