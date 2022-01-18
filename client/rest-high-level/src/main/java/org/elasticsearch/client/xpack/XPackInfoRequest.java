/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.xpack;

import org.elasticsearch.client.Validatable;

import java.util.EnumSet;
import java.util.Locale;

/**
 * Fetch information about X-Pack from the cluster.
 */
public class XPackInfoRequest implements Validatable {

    public enum Category {
        BUILD,
        LICENSE,
        FEATURES;

        public static EnumSet<Category> toSet(String... categories) {
            EnumSet<Category> set = EnumSet.noneOf(Category.class);
            for (String category : categories) {
                switch (category) {
                    case "_all":
                        return EnumSet.allOf(Category.class);
                    case "_none":
                        return EnumSet.noneOf(Category.class);
                    default:
                        set.add(Category.valueOf(category.toUpperCase(Locale.ROOT)));
                }
            }
            return set;
        }
    }

    private boolean verbose;
    private EnumSet<Category> categories = EnumSet.noneOf(Category.class);

    public XPackInfoRequest() {}

    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }

    public boolean isVerbose() {
        return verbose;
    }

    public void setCategories(EnumSet<Category> categories) {
        this.categories = categories;
    }

    public EnumSet<Category> getCategories() {
        return categories;
    }

}
