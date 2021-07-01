/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.painless.phase.UserTreeVisitor;
import org.elasticsearch.painless.symbol.ScriptScope;
import org.elasticsearch.painless.toxcontent.UserTreeToXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ToXContentTests extends ScriptTestCase {
    public void testUserFunction() {
        Map<?,?> func = getFunction("def twofive(int i) { return 25 + i; } int j = 23; twofive(j)", "twofive");
        assertFalse((Boolean)func.get("isInternal"));
        assertFalse((Boolean)func.get("isStatic"));
        assertEquals("SFunction", func.get("node"));
        assertEquals("def", func.get("returns"));
        assertEquals(List.of("int"), func.get("parameterTypes"));
        assertEquals(List.of("i"), func.get("parameters"));
    }

    public void testBlock() {
        Map<?, ?> execute = getExecute("int i = 5; return i;");
        Map<?, ?> block = getNode(execute, "block", "SBlock");
        for (Object obj : (List<?>) block.get("statements")) {
            Map<?, ?> statement = (Map<?, ?>) obj;
        }
        Map<?, ?> decl = getStatement(block, "SDeclBlock");
        List<?> decls = (List<?>) decl.get("declarations");
        assertEquals(1, decls.size());
        assertEquals("i", ((Map<?,?>) decls.get(0)).get("symbol"));
        assertEquals("int", ((Map<?,?>) decls.get(0)).get("type"));

        Map<?, ?> ret = getStatement(block, "SReturn");
        Map<?, ?> symbol = (Map<?, ?>)((List<?>) ret.get("value")).get(0);
        assertEquals("ESymbol", symbol.get("node"));
        assertEquals("i", symbol.get("symbol"));
    }

    public void testFor() {
        Map<?, ?> execute = getExecute("int q = 0; for (int j = 0; j < 100; j++) { q += j; } return q");
        Map<?, ?> sfor = getStatement(getNode(execute, "block", "SBlock"), "SFor");

        Map<?, ?> ecomp = getNode(sfor, "condition", "EComp");
        assertEquals("j", getNode(ecomp, "left", "ESymbol").get("symbol"));
        assertEquals("100", getNode(ecomp, "right", "ENumeric").get("numeric"));
        assertEquals("less than", ((Map<?,?>) ecomp.get("operation")).get("name"));

        Map<?, ?> init = getNode(sfor, "initializer", "SDeclBlock");
        Map<?, ?> decl = getNode(init, "declarations", "SDeclaration");
        assertEquals("j", decl.get("symbol"));
        assertEquals("int", decl.get("type"));
        assertEquals("0", getNode(decl, "value", "ENumeric").get("numeric"));

        Map<?, ?> after = getNode(sfor, "afterthought", "EAssignment");
        assertEquals("j", getNode(after, "left", "ESymbol").get("symbol"));
        assertEquals("1", getNode(after, "right", "ENumeric").get("numeric"));
        assertTrue((Boolean)after.get("postIfRead"));
    }

    private Map<?, ?> getStatement(Map<?, ?> block, String node) {
        return getNode(block, "statements", node);
    }

    private Map<?, ?> getNode(Map<?, ?> map, String key, String node) {
        for (Object obj : (List<?>) map.get(key)) {
            Map<?, ?> nodeMap = (Map<?, ?>) obj;
            if (node.equals(nodeMap.get("node"))) {
                return nodeMap;
            }
        }
        fail("Missing node [" + node + "]");
        return Collections.emptyMap();
    }

    private Map<?, ?> getExecute(String script) {
        return getFunction(script, "execute");
    }

    private Map<?, ?> getFunction(String script, String function) {
        return getFunction(semanticPhase(script), function);
    }

    private Map<?, ?> getFunction(XContentBuilder builder, String function) {
        Map<String, Object> map = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
        for (Object funcObj: ((List<?>)map.get("functions"))) {
            if (funcObj instanceof Map) {
                if (function.equals(((Map<?, ?>) funcObj).get("name"))) {
                    return (Map<?, ?>) funcObj;
                }
            }
        }
        fail("Function [" + function + "] not found");
        return Collections.emptyMap();
    }

    private XContentBuilder semanticPhase(String script) {
        XContentBuilder builder;
        try {
            builder = XContentFactory.jsonBuilder();
        } catch (IOException err) {
            fail("script [" + script + "] threw IOException [" + err.getMessage() + "]");
            return null;
        }
        UserTreeVisitor<ScriptScope> semantic = new UserTreeToXContent(builder);
        Debugger.phases(script, semantic, null, null);
        Map<String, Object> map = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
        assertEquals(script, map.get("source"));
        return builder;
    }
}
