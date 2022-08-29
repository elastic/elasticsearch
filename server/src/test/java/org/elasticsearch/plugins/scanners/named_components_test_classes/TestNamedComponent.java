/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.scanners.named_components_test_classes;

import org.elasticsearch.plugins.scanners.extensible_test_classes.ExtensibleInterface;

@org.elasticsearch.plugin.api.NamedComponent(name = "test_named_component")
public class TestNamedComponent implements ExtensibleInterface {

}
