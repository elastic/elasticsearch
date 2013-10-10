/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.ElasticSearchParseException;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.unit.TimeValue;

import static org.elasticsearch.common.unit.ByteSizeValue.parseBytesSizeValue;

/**
 * Validates a setting, returning a failure message if applicable.
 */
public interface Validator {

    String validate(String setting, String value);

    public static final Validator EMPTY = new Validator() {
        @Override
        public String validate(String setting, String value) {
            return null;
        }
    };

    public static final Validator TIME = new Validator() {
        @Override
        public String validate(String setting, String value) {
            try {
                if (TimeValue.parseTimeValue(value, null) == null) {
                    return "cannot parse value [" + value + "] as time";
                }
            } catch (ElasticSearchParseException ex) {
                return "cannot parse value [" + value + "] as time";
            }
            return null;
        }
    };

    public static final Validator FLOAT = new Validator() {
        @Override
        public String validate(String setting, String value) {
            try {
                Float.parseFloat(value);
            } catch (NumberFormatException ex) {
                return "cannot parse value [" + value + "] as a float";
            }
            return null;
        }
    };

    public static final Validator NON_NEGATIVE_FLOAT = new Validator() {
        @Override
        public String validate(String setting, String value) {
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

    public static final Validator DOUBLE = new Validator() {
        @Override
        public String validate(String setting, String value) {
            try {
                Double.parseDouble(value);
            } catch (NumberFormatException ex) {
                return "cannot parse value [" + value + "] as a double";
            }
            return null;
        }
    };

    public static final Validator NON_NEGATIVE_DOUBLE = new Validator() {
        @Override
        public String validate(String setting, String value) {
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

    public static final Validator DOUBLE_GTE_2 = new Validator() {
        @Override
        public String validate(String setting, String value) {
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

    public static final Validator INTEGER = new Validator() {
        @Override
        public String validate(String setting, String value) {
            try {
                Integer.parseInt(value);
            } catch (NumberFormatException ex) {
                return "cannot parse value [" + value + "] as an integer";
            }
            return null;
        }
    };

    public static final Validator POSITIVE_INTEGER = new Validator() {
        @Override
        public String validate(String setting, String value) {
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

    public static final Validator NON_NEGATIVE_INTEGER = new Validator() {
        @Override
        public String validate(String setting, String value) {
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

    public static final Validator INTEGER_GTE_2 = new Validator() {
        @Override
        public String validate(String setting, String value) {
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

    public static final Validator BYTES_SIZE = new Validator() {
        @Override
        public String validate(String setting, String value) {
            try {
                parseBytesSizeValue(value);
            } catch (ElasticSearchParseException ex) {
                return ex.getMessage();
            }
            return null;
        }
    };
    
    public static final Validator BOOLEAN = new Validator() {
        @Override
        public String validate(String setting, String value) {

            if (value != null && (Booleans.isExplicitFalse(value) || Booleans.isExplicitTrue(value))) {
                return null;
            }
            return "cannot parse value [" + value + "] as a boolean";
        }
    };
}
