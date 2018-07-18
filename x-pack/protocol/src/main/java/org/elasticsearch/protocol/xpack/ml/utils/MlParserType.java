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
package org.elasticsearch.protocol.xpack.ml.utils;

/**
 * In order to allow enhancements that require additions to the ML custom cluster state to be made in minor versions,
 * when we parse our metadata from persisted cluster state we ignore unknown fields.  However, we don't want to be
 * lenient when parsing config as this would mean user mistakes could go undetected.  Therefore, for all JSON objects
 * that are used in both custom cluster state and config we have two parsers, one tolerant of unknown fields (for
 * parsing cluster state) and one strict (for parsing config).  This class enumerates the two options.
 */
public enum MlParserType {

    METADATA, CONFIG;

}
