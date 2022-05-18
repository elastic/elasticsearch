/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.VersionType;

import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/** Metadata about an ingestion operation, implemented by scripting contexts */
public abstract class Metadata {
    public static final String INDEX = "_index";
    public static final String ID = "_id";
    public static final String VERSION = "_version";
    public static final String ROUTING = "_routing";
    public static final String OP = "op";
    public static final String VERSION_TYPE = "_version_type";
    public static final String SOURCE = "_source";

    protected boolean validOp(Op op) {
        return false;
    }

    protected Op opFromString(String strOp) {
        return Op.fromString(strOp);
    }
}
