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

package org.elasticsearch.cluster.settings;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import static org.elasticsearch.common.unit.ByteSizeValue.parseBytesSizeValue;
import static org.elasticsearch.common.unit.MemorySizeValue.parseBytesSizeValueOrHeapRatio;


/**
 * Validates a setting, returning a failure message if applicable.
 */
public interface Validator {

    String validate(String setting, String value, ClusterState clusterState);

    Validator EMPTY = new Validator() {
        @Override
        public String validate(String setting, String value, ClusterState clusterState) {
            return null;
        }
    };

    Validator TIME = new Validator() {
        @Override
        public String validate(String setting, String value, ClusterState clusterState) {
            if (value == null) {
                throw new NullPointerException("value must not be null");
            }
            try {
                // This never returns null:
                TimeValue.parseTimeValue(value, null, setting);
            } catch (ElasticsearchParseException ex) {
                return ex.getMessage();
            }
            return null;
        }
    };

    Validator TIMEOUT = new Validator() {
        @Override
        public String validate(String setting, String value, ClusterState clusterState) {
            try {
                if (value == null) {
                    throw new NullPointerException("value must not be null");
                }
                TimeValue timeValue = TimeValue.parseTimeValue(value, null, setting);
                assert timeValue != null;
                if (timeValue.millis() < 0 && timeValue.millis() != -1) {
                    return "cannot parse value [" + value + "] as a timeout";
                }
            } catch (ElasticsearchParseException ex) {
                return ex.getMessage();
            }
            return null;
        }
    };

    Validator TIME_NON_NEGATIVE = new Validator() {
        @Override
        public String validate(String setting, String value, ClusterState clusterState) {
            try {
                if (value == null) {
                    throw new NullPointerException("value must not be null");
                }
                TimeValue timeValue = TimeValue.parseTimeValue(value, null, setting);
                assert timeValue != null;
                if (timeValue.millis() < 0) {
                    return "cannot parse value [" + value + "] as non negative time";
                }
            } catch (ElasticsearchParseException ex) {
                return ex.getMessage();
            }
            return null;
        }
    };

    Validator FLOAT = new Validator() {
        @Override
        public String validate(String setting, String value, ClusterState clusterState) {
            try {
                Float.parseFloat(value);
            } catch (NumberFormatException ex) {
                return "cannot parse value [" + value + "] as a float";
            }
            return null;
        }
    };

    Validator NON_NEGATIVE_FLOAT = new Validator() {
        @Override
        public String validate(String setting, String value, ClusterState clusterState) {
            try {
                if (Float.parseFloat(value) < 0.0) {
                    return "the value of the setting " + setting + " must be a non negative float";
                }
            } catch (NumberFormatException ex) {
                return "cannot parse value [" + value + "] as a double";
            }
            return null;
        }
    };

    Validator DOUBLE = new Validator() {
        @Override
        public String validate(String setting, String value, ClusterState clusterState) {
            try {
                Double.parseDouble(value);
            } catch (NumberFormatException ex) {
                return "cannot parse value [" + value + "] as a double";
            }
            return null;
        }
    };

    Validator NON_NEGATIVE_DOUBLE = new Validator() {
        @Override
        public String validate(String setting, String value, ClusterState clusterState) {
            try {
                if (Double.parseDouble(value) < 0.0) {
                    return "the value of the setting " + setting + " must be a non negative double";
                }
            } catch (NumberFormatException ex) {
                return "cannot parse value [" + value + "] as a double";
            }
            return null;
        }
    };

    Validator DOUBLE_GTE_2 = new Validator() {
        @Override
        public String validate(String setting, String value, ClusterState clusterState) {
            try {
                if (Double.parseDouble(value) < 2.0) {
                    return "the value of the setting " + setting + " must be >= 2.0";
                }
            } catch (NumberFormatException ex) {
                return "cannot parse value [" + value + "] as a double";
            }
            return null;
        }
    };

    Validator INTEGER = new Validator() {
        @Override
        public String validate(String setting, String value, ClusterState clusterState) {
            try {
                Integer.parseInt(value);
            } catch (NumberFormatException ex) {
                return "cannot parse value [" + value + "] as an integer";
            }
            return null;
        }
    };

    Validator POSITIVE_INTEGER = new Validator() {
        @Override
        public String validate(String setting, String value, ClusterState clusterState) {
            try {
                if (Integer.parseInt(value) <= 0) {
                    return "the value of the setting " + setting + " must be a positive integer";
                }
            } catch (NumberFormatException ex) {
                return "cannot parse value [" + value + "] as an integer";
            }
            return null;
        }
    };

    Validator NON_NEGATIVE_INTEGER = new Validator() {
        @Override
        public String validate(String setting, String value, ClusterState clusterState) {
            try {
                if (Integer.parseInt(value) < 0) {
                    return "the value of the setting " + setting + " must be a non negative integer";
                }
            } catch (NumberFormatException ex) {
                return "cannot parse value [" + value + "] as an integer";
            }
            return null;
        }
    };

    Validator INTEGER_GTE_2 = new Validator() {
        @Override
        public String validate(String setting, String value, ClusterState clusterState) {
            try {
                if (Integer.parseInt(value) < 2) {
                    return "the value of the setting " + setting + " must be >= 2";
                }
            } catch (NumberFormatException ex) {
                return "cannot parse value [" + value + "] as an integer";
            }
            return null;
        }
    };

    Validator BYTES_SIZE = new Validator() {
        @Override
        public String validate(String setting, String value, ClusterState clusterState) {
            try {
                parseBytesSizeValue(value, setting);
            } catch (ElasticsearchParseException ex) {
                return ex.getMessage();
            }
            return null;
        }
    };

    Validator POSITIVE_BYTES_SIZE = new Validator() {
        @Override
        public String validate(String setting, String value, ClusterState state) {
            try {
                ByteSizeValue byteSizeValue = parseBytesSizeValue(value, setting);
                if (byteSizeValue.getBytes() <= 0) {
                    return setting + " must be a positive byte size value";
                }
            } catch (ElasticsearchParseException ex) {
                return ex.getMessage();
            }
            return null;
        }
    };

    Validator PERCENTAGE = new Validator() {
        @Override
        public String validate(String setting, String value, ClusterState clusterState) {
            try {
                if (value == null) {
                    return "the value of " + setting + " can not be null";
                }
                if (!value.endsWith("%")) {
                    return "the value [" + value + "] for " + setting + " must end with %";
                }
                final double asDouble = Double.parseDouble(value.substring(0, value.length() - 1));
                if (asDouble < 0.0 || asDouble > 100.0) {
                    return "the value [" + value + "] for " + setting + " must be a percentage between 0% and 100%";
                }
            } catch (NumberFormatException ex) {
                return ex.getMessage();
            }
            return null;
        }
    };


    Validator BYTES_SIZE_OR_PERCENTAGE = new Validator() {
        @Override
        public String validate(String setting, String value, ClusterState clusterState) {
            String byteSize = BYTES_SIZE.validate(setting, value, clusterState);
            if (byteSize != null) {
                String percentage = PERCENTAGE.validate(setting, value, clusterState);
                if (percentage == null) {
                    return null;
                }
                return percentage + " or be a valid bytes size value, like [16mb]";
            }
            return null;
        }
    };


    Validator MEMORY_SIZE = new Validator() {
        @Override
        public String validate(String setting, String value, ClusterState clusterState) {
            try {
                parseBytesSizeValueOrHeapRatio(value, setting);
            } catch (ElasticsearchParseException ex) {
                return ex.getMessage();
            }
            return null;
        }
    };

    public static final Validator BOOLEAN = new Validator() {
        @Override
        public String validate(String setting, String value, ClusterState clusterState) {

            if (value != null && (Booleans.isExplicitFalse(value) || Booleans.isExplicitTrue(value))) {
                return null;
            }
            return "cannot parse value [" + value + "] as a boolean";
        }
    };
}
