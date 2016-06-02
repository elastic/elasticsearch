/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.ingest.core;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ingest.ProcessorsRegistry;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public final class ConfigurationUtils {

    private ConfigurationUtils() {
    }

    /**
     * Returns and removes the specified optional property from the specified configuration map.
     *
     * If the property value isn't of type string a {@link ElasticsearchParseException} is thrown.
     */
    public static String readOptionalStringProperty(String processorType, String processorTag, Map<String, Object> configuration,
                                                    String propertyName) {
        Object value = configuration.remove(propertyName);
        return readString(processorType, processorTag, propertyName, value);
    }

    /**
     * Returns and removes the specified property from the specified configuration map.
     *
     * If the property value isn't of type string an {@link ElasticsearchParseException} is thrown.
     * If the property is missing an {@link ElasticsearchParseException} is thrown
     */
    public static String readStringProperty(String processorType, String processorTag, Map<String, Object> configuration,
                                            String propertyName) {
        return readStringProperty(processorType, processorTag, configuration, propertyName, null);
    }

    /**
     * Returns and removes the specified property from the specified configuration map.
     *
     * If the property value isn't of type string a {@link ElasticsearchParseException} is thrown.
     * If the property is missing and no default value has been specified a {@link ElasticsearchParseException} is thrown
     */
    public static String readStringProperty(String processorType, String processorTag, Map<String, Object> configuration,
                                            String propertyName, String defaultValue) {
        Object value = configuration.remove(propertyName);
        if (value == null && defaultValue != null) {
            return defaultValue;
        } else if (value == null) {
            throw newConfigurationException(processorType, processorTag, propertyName, "required property is missing");
        }
        return readString(processorType, processorTag, propertyName, value);
    }

    private static String readString(String processorType, String processorTag, String propertyName, Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String) {
            return (String) value;
        }
        throw newConfigurationException(processorType, processorTag, propertyName, "property isn't a string, but of type [" +
            value.getClass().getName() + "]");
    }

    public static Boolean readBooleanProperty(String processorType, String processorTag, Map<String, Object> configuration,
                                             String propertyName, boolean defaultValue) {
        Object value = configuration.remove(propertyName);
        if (value == null) {
            return defaultValue;
        } else {
            return readBoolean(processorType, processorTag, propertyName, value).booleanValue();
        }
    }

    private static Boolean readBoolean(String processorType, String processorTag, String propertyName, Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        throw newConfigurationException(processorType, processorTag, propertyName, "property isn't a boolean, but of type [" +
            value.getClass().getName() + "]");
    }


    /**
     * Returns and removes the specified property from the specified configuration map.
     *
     * If the property value isn't of type int a {@link ElasticsearchParseException} is thrown.
     * If the property is missing an {@link ElasticsearchParseException} is thrown
     */
    public static int readIntProperty(String processorType, String processorTag, Map<String, Object> configuration, String propertyName,
                                      int defaultValue) {
        Object value = configuration.remove(propertyName);
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.toString());
        } catch (Throwable t) {
            throw newConfigurationException(processorType, processorTag, propertyName,
                "property cannot be converted to an int [" + value.toString() + "]");
        }
    }

    /**
     * Returns and removes the specified property of type list from the specified configuration map.
     *
     * If the property value isn't of type list an {@link ElasticsearchParseException} is thrown.
     */
    public static <T> List<T> readOptionalList(String processorType, String processorTag, Map<String, Object> configuration,
                                               String propertyName) {
        Object value = configuration.remove(propertyName);
        if (value == null) {
            return null;
        }
        return readList(processorType, processorTag, propertyName, value);
    }

    /**
     * Returns and removes the specified property of type list from the specified configuration map.
     *
     * If the property value isn't of type list an {@link ElasticsearchParseException} is thrown.
     * If the property is missing an {@link ElasticsearchParseException} is thrown
     */
    public static <T> List<T> readList(String processorType, String processorTag, Map<String, Object> configuration, String propertyName) {
        Object value = configuration.remove(propertyName);
        if (value == null) {
            throw newConfigurationException(processorType, processorTag, propertyName, "required property is missing");
        }

        return readList(processorType, processorTag, propertyName, value);
    }

    private static <T> List<T> readList(String processorType, String processorTag, String propertyName, Object value) {
        if (value instanceof List) {
            @SuppressWarnings("unchecked")
            List<T> stringList = (List<T>) value;
            return stringList;
        } else {
            throw newConfigurationException(processorType, processorTag, propertyName,
                "property isn't a list, but of type [" + value.getClass().getName() + "]");
        }
    }

    /**
     * Returns and removes the specified property of type map from the specified configuration map.
     *
     * If the property value isn't of type map an {@link ElasticsearchParseException} is thrown.
     * If the property is missing an {@link ElasticsearchParseException} is thrown
     */
    public static <T> Map<String, T> readMap(String processorType, String processorTag, Map<String, Object> configuration,
                                             String propertyName) {
        Object value = configuration.remove(propertyName);
        if (value == null) {
            throw newConfigurationException(processorType, processorTag, propertyName, "required property is missing");
        }

        return readMap(processorType, processorTag, propertyName, value);
    }

    /**
     * Returns and removes the specified property of type map from the specified configuration map.
     *
     * If the property value isn't of type map an {@link ElasticsearchParseException} is thrown.
     */
    public static <T> Map<String, T> readOptionalMap(String processorType, String processorTag, Map<String, Object> configuration,
                                                     String propertyName) {
        Object value = configuration.remove(propertyName);
        if (value == null) {
            return null;
        }

        return readMap(processorType, processorTag, propertyName, value);
    }

    private static <T> Map<String, T> readMap(String processorType, String processorTag, String propertyName, Object value) {
        if (value instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, T> map = (Map<String, T>) value;
            return map;
        } else {
            throw newConfigurationException(processorType, processorTag, propertyName,
                "property isn't a map, but of type [" + value.getClass().getName() + "]");
        }
    }

    /**
     * Returns and removes the specified property as an {@link Object} from the specified configuration map.
     */
    public static Object readObject(String processorType, String processorTag, Map<String, Object> configuration, String propertyName) {
        Object value = configuration.remove(propertyName);
        if (value == null) {
            throw newConfigurationException(processorType, processorTag, propertyName, "required property is missing");
        }
        return value;
    }

    public static ElasticsearchParseException newConfigurationException(String processorType, String processorTag, String propertyName,
                                                                        String reason) {
        ElasticsearchParseException exception = new ElasticsearchParseException("[" + propertyName + "] " + reason);

        if (processorType != null) {
            exception.addHeader("processor_type", processorType);
        }
        if (processorTag != null) {
            exception.addHeader("processor_tag", processorTag);
        }
        if (propertyName != null) {
            exception.addHeader("property_name", propertyName);
        }
        return exception;
    }

    public static List<Processor> readProcessorConfigs(List<Map<String, Map<String, Object>>> processorConfigs,
                                                       ProcessorsRegistry processorRegistry) throws Exception {
        List<Processor> processors = new ArrayList<>();
        if (processorConfigs != null) {
            for (Map<String, Map<String, Object>> processorConfigWithKey : processorConfigs) {
                for (Map.Entry<String, Map<String, Object>> entry : processorConfigWithKey.entrySet()) {
                    processors.add(readProcessor(processorRegistry, entry.getKey(), entry.getValue()));
                }
            }
        }
        return processors;
    }

    private static Processor readProcessor(ProcessorsRegistry processorRegistry, String type, Map<String, Object> config) throws Exception {
        Processor.Factory factory = processorRegistry.getProcessorFactory(type);
        if (factory != null) {
            boolean ignoreFailure = ConfigurationUtils.readBooleanProperty(null, null, config, "ignore_failure", false);
            List<Map<String, Map<String, Object>>> onFailureProcessorConfigs =
                ConfigurationUtils.readOptionalList(null, null, config, Pipeline.ON_FAILURE_KEY);

            List<Processor> onFailureProcessors = readProcessorConfigs(onFailureProcessorConfigs, processorRegistry);
            Processor processor;
            processor = factory.create(config);
            if (!config.isEmpty()) {
                throw new ElasticsearchParseException("processor [{}] doesn't support one or more provided configuration parameters {}",
                    type, Arrays.toString(config.keySet().toArray()));
            }
            if (onFailureProcessors.size() > 0 || ignoreFailure) {
                return new CompoundProcessor(ignoreFailure, Collections.singletonList(processor), onFailureProcessors);
            } else {
                return processor;
            }
        }
        throw new ElasticsearchParseException("No processor type exists with name [" + type + "]");
    }
}
