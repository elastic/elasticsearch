/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/**
 * An api used by plugin developers to implement custom Elasticsearch plugins.
 * The package convention in plugin apis is as follows:
 * <ul>
 *      <li> The root package is org.elasticsearch.plugin</li>
 *      <li> Specialised API jars have their name following the root package.
 *           Interfaces and annotations used by plugin developers should be placed under it.
 *           i.e. org.elasticsearch.plugin.analysis
 *      </li>
 *      <li> packages which are not meant to be used by plugin developers should be under internal package suffix
 *           i.e org.elasticsearch.plugin.analysis.internal
 *      </li>
 * </ul>
 */
package org.elasticsearch.plugin;
