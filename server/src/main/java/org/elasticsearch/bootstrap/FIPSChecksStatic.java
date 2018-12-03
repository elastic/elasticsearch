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

package org.elasticsearch.bootstrap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.Constants;
import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.transport.BoundTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.monitor.process.ProcessProbe;
import org.elasticsearch.node.NodeValidationException;
import org.elasticsearch.xpack.security.FIPSChecks;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.AllPermission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * We enforce bootstrap FIPS checks always.
 */
final class FIPSChecksStatic {

    private FIPSChecksStatic() { }
    
    /**
     * The other two methods are not necessary for a FIPS check since they are always enforced.
     */
    
    /**
     * Executes the provided checks and fails if necessary.
     *
     * @param context the current node boostrap context
     * @param checks        the checks to execute
     * @param logger        the logger to
     */
    static void check(
        final BootstrapContext context,
        final FIPSChecks checks,
        final Environment env,
        final Logger logger) throws NodeValidationException {
        final List<String> errors = new ArrayList<>();
        final List<String> ignoredErrors = new ArrayList<>();

        logger.info("explicitly enforcing FIPS checks");
        final FIPSChecks.FIPSCheckResult result = checks.check(context, env);

        if (result.isFailure()) {
            errors.add(result.getMessage());
        }

        if (!ignoredErrors.isEmpty()) {
            ignoredErrors.forEach(error -> log(logger, error));
        }

        if (!errors.isEmpty()) {
            final List<String> messages = new ArrayList<>(1 + errors.size());
            messages.add("[" + errors.size() + "] bootstrap checks failed");
            for (int i = 0; i < errors.size(); i++) {
                messages.add("[" + (i + 1) + "]: " + errors.get(i));
            }
            final NodeValidationException ne = new NodeValidationException(String.join("\n", messages));
            errors.stream().map(IllegalStateException::new).forEach(ne::addSuppressed);
            throw ne;
        }
    }

    static void log(final Logger logger, final String error) {
        logger.warn(error);
    }

}
