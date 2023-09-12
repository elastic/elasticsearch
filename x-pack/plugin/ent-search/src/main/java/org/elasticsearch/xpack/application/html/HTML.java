/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.html;

public class HTML {

    public static Element parse(String html){
        return new Element();
    }

    public static class Element {
        public Element getElementAt(String selector){
            return new Element(); // TODO
        }

        public String getText(){
            return "hello, world"; // TODO
        }
    }
}
