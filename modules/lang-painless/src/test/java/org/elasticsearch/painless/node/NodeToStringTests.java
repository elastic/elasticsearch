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

package org.elasticsearch.painless.node;

import org.elasticsearch.painless.CompilerSettings;
import org.elasticsearch.painless.FeatureTestObject;
import org.elasticsearch.painless.Locals.Variable;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.ScriptClassInfo;
import org.elasticsearch.painless.action.PainlessExecuteAction.PainlessTestScript;
import org.elasticsearch.painless.antlr.Walker;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessClass;
import org.elasticsearch.painless.lookup.PainlessField;
import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.painless.lookup.PainlessLookupBuilder;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.spi.Whitelist;
import org.elasticsearch.painless.spi.WhitelistLoader;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * Tests {@link Object#toString} implementations on all extensions of {@link ANode}.
 */
public class NodeToStringTests extends ESTestCase {

    public void testEAssignment() {
        assertToString(
                "(SSource\n"
              + "  (SDeclBlock (SDeclaration def i))\n"
              + "  (SExpression (EAssignment (EVariable i) = (ENumeric 2)))\n"
              + "  (SReturn (EVariable i)))",
                "def i;\n"
              + "i = 2;\n"
              + "return i");
        for (String operator : new String[] {"+", "-", "*", "/", "%", "&", "^", "|", "<<", ">>", ">>>"}) {
            assertToString(
                    "(SSource\n"
                  + "  (SDeclBlock (SDeclaration def i (ENumeric 1)))\n"
                  + "  (SExpression (EAssignment (EVariable i) " + operator + "= (ENumeric 2)))\n"
                  + "  (SReturn (EVariable i)))",
                    "def i = 1;\n"
                  + "i " + operator + "= 2;\n"
                  + "return i");
        }
        // Compound
        assertToString(
                "(SSource\n"
              + "  (SDeclBlock (SDeclaration def i))\n"
              + "  (SReturn (EAssignment (EVariable i) = (ENumeric 2))))",
                "def i;\n"
              + "return i = 2");
        assertToString(
                "(SSource\n"
              + "  (SDeclBlock (SDeclaration def i))\n"
              + "  (SReturn (EAssignment (EVariable i) ++ post)))",
                "def i;\n"
              + "return i++");
        assertToString(
                "(SSource\n"
              + "  (SDeclBlock (SDeclaration def i))\n"
              + "  (SReturn (EAssignment (EVariable i) ++ pre)))",
                "def i;\n"
              + "return ++i");
        assertToString(
                "(SSource\n"
              + "  (SDeclBlock (SDeclaration def i))\n"
              + "  (SReturn (EAssignment (EVariable i) -- post)))",
                "def i;\n"
              + "return i--");
        assertToString(
                "(SSource\n"
              + "  (SDeclBlock (SDeclaration def i))\n"
              + "  (SReturn (EAssignment (EVariable i) -- pre)))",
                "def i;\n"
              + "return --i");
    }

    public void testEBinary() {
        assertToString(     "(SSource (SReturn (EBinary (ENumeric 1) * (ENumeric 1))))", "return 1 * 1");
        assertToString(     "(SSource (SReturn (EBinary (ENumeric 1) / (ENumeric 1))))", "return 1 / 1");
        assertToString(     "(SSource (SReturn (EBinary (ENumeric 1) % (ENumeric 1))))", "return 1 % 1");
        assertToString(     "(SSource (SReturn (EBinary (ENumeric 1) + (ENumeric 1))))", "return 1 + 1");
        assertToString(     "(SSource (SReturn (EBinary (ENumeric 1) - (ENumeric 1))))", "return 1 - 1");
        assertToString( "(SSource (SReturn (EBinary (EString 'asb') =~ (ERegex /cat/))))", "return 'asb' =~ /cat/");
        assertToString("(SSource (SReturn (EBinary (EString 'asb') ==~ (ERegex /cat/))))", "return 'asb' ==~ /cat/");
        assertToString(    "(SSource (SReturn (EBinary (ENumeric 1) << (ENumeric 1))))", "return 1 << 1");
        assertToString(    "(SSource (SReturn (EBinary (ENumeric 1) >> (ENumeric 1))))", "return 1 >> 1");
        assertToString(   "(SSource (SReturn (EBinary (ENumeric 1) >>> (ENumeric 1))))", "return 1 >>> 1");
        assertToString(     "(SSource (SReturn (EBinary (ENumeric 1) & (ENumeric 1))))", "return 1 & 1");
        assertToString(     "(SSource (SReturn (EBinary (ENumeric 1) ^ (ENumeric 1))))", "return 1 ^ 1");
        assertToString(     "(SSource (SReturn (EBinary (ENumeric 1) | (ENumeric 1))))", "return 1 | 1");
    }

    public void testEBool() {
        assertToString("(SSource (SReturn (EBool (EBoolean true) && (EBoolean false))))", "return true && false");
        assertToString("(SSource (SReturn (EBool (EBoolean true) || (EBoolean false))))", "return true || false");
    }

    public void testEBoolean() {
        assertToString("(SSource (SReturn (EBoolean true)))",  "return true");
        assertToString("(SSource (SReturn (EBoolean false)))", "return false");
    }

    public void testECallLocal() {
        assertToString(
                "(SSource\n"
              + "  (SFunction def a\n"
              + "    (SReturn (EBoolean true)))\n"
              + "  (SReturn (ECallLocal a)))",
                "def a() {\n"
              + "  return true\n"
              + "}\n"
              + "return a()");
        assertToString(
                "(SSource\n"
              + "  (SFunction def a (Args (Pair int i) (Pair int j))\n"
              + "    (SReturn (EBoolean true)))\n"
              + "  (SReturn (ECallLocal a (Args (ENumeric 1) (ENumeric 2)))))",
                "def a(int i, int j) {\n"
              + "  return true\n"
              + "}\n"
              + "return a(1, 2)");
    }

    public void testECapturingFunctionRef() {
        assertToString(
                  "(SSource\n"
                + "  (SDeclBlock (SDeclaration Integer x (PCallInvoke (EStatic Integer) valueOf (Args (ENumeric 5)))))\n"
                + "  (SReturn (PCallInvoke (PCallInvoke (EStatic Optional) empty) orElseGet (Args (ECapturingFunctionRef x toString)))))",
                  "Integer x = Integer.valueOf(5);\n"
                + "return Optional.empty().orElseGet(x::toString)");
    }

    public void testECast() {
        Location l = new Location(getTestName(), 0);
        AExpression child = new EConstant(l, "test");
        PainlessCast cast = PainlessCast.originalTypetoTargetType(String.class, Integer.class, true);
        assertEquals("(ECast java.lang.Integer (EConstant String 'test'))", new ECast(l, child, cast).toString());

        l = new Location(getTestName(), 1);
        child = new EBinary(l, Operation.ADD, new EConstant(l, "test"), new EConstant(l, 12));
        cast = PainlessCast.originalTypetoTargetType(Integer.class, Boolean.class, true);
        assertEquals("(ECast java.lang.Boolean (EBinary (EConstant String 'test') + (EConstant Integer 12)))",
            new ECast(l, child, cast).toString());
    }

    public void testEComp() {
        assertToString(  "(SSource (SReturn (EComp (PField (EVariable params) a) < (ENumeric 10))))", "return params.a < 10");
        assertToString( "(SSource (SReturn (EComp (PField (EVariable params) a) <= (ENumeric 10))))", "return params.a <= 10");
        assertToString(  "(SSource (SReturn (EComp (PField (EVariable params) a) > (ENumeric 10))))", "return params.a > 10");
        assertToString( "(SSource (SReturn (EComp (PField (EVariable params) a) >= (ENumeric 10))))", "return params.a >= 10");
        assertToString( "(SSource (SReturn (EComp (PField (EVariable params) a) == (ENumeric 10))))", "return params.a == 10");
        assertToString("(SSource (SReturn (EComp (PField (EVariable params) a) === (ENumeric 10))))", "return params.a === 10");
        assertToString( "(SSource (SReturn (EComp (PField (EVariable params) a) != (ENumeric 10))))", "return params.a != 10");
        assertToString("(SSource (SReturn (EComp (PField (EVariable params) a) !== (ENumeric 10))))", "return params.a !== 10");
    }

    public void testEConditional() {
        assertToString("(SSource (SReturn (EConditional (PField (EVariable params) a) (ENumeric 1) (ENumeric 6))))",
                "return params.a ? 1 : 6");
    }

    public void testEConstant() {
        assertEquals("(EConstant String '121')", new EConstant(new Location(getTestName(), 0), "121").toString());
        assertEquals("(EConstant String '92 ')", new EConstant(new Location(getTestName(), 0), "92 ").toString());
        assertEquals("(EConstant Integer 1237)", new EConstant(new Location(getTestName(), 1), 1237).toString());
        assertEquals("(EConstant Boolean true)", new EConstant(new Location(getTestName(), 2), true).toString());
    }

    public void testEDecimal() {
        assertToString("(SSource (SReturn (EDecimal 1.0)))", "return 1.0");
        assertToString("(SSource (SReturn (EDecimal 14.121d)))", "return 14.121d");
        assertToString("(SSource (SReturn (EDecimal 2234.1f)))", "return 2234.1f");
        assertToString("(SSource (SReturn (EDecimal 14.121D)))", "return 14.121D");
        assertToString("(SSource (SReturn (EDecimal 1234.1F)))", "return 1234.1F");
    }

    public void testEElvis() {
        assertToString("(SSource (SReturn (EElvis (PField (EVariable params) a) (ENumeric 1))))", "return params.a ?: 1");
    }

    public void testEExplicit() {
        assertToString("(SSource (SReturn (EExplicit byte (PField (EVariable params) a))))", "return (byte)(params.a)");
    }

    public void testEFunctionRef() {
        assertToString(
                "(SSource (SReturn "
                        + "(PCallInvoke (PCallInvoke (EStatic Optional) empty) orElseGet (Args (EFunctionRef Optional empty)))))",
                "return Optional.empty().orElseGet(Optional::empty)");
    }

    public void testEInstanceOf() {
        assertToString("(SSource (SReturn (EInstanceof (ENewObj Object) Object)))", "return new Object() instanceof Object");
        assertToString("(SSource (SReturn (EInstanceof (ENumeric 12) double)))", "return 12 instanceof double");
    }

    public void testELambda() {
        assertToString(
                  "(SSource (SReturn (PCallInvoke (PCallInvoke (EStatic Optional) empty) orElseGet (Args "
                + "(ELambda (SReturn (ENumeric 1)))))))",
                  "return Optional.empty().orElseGet(() -> {\n"
                + "  return 1\n"
                + "})");
        assertToString(
                  "(SSource (SReturn (PCallInvoke (PCallInvoke (EStatic Optional) empty) orElseGet (Args "
                + "(ELambda (SReturn (ENumeric 1)))))))",
                  "return Optional.empty().orElseGet(() -> 1)");
        assertToString(
                  "(SSource (SReturn (PCallInvoke (PCallInvoke (PCallInvoke (EListInit (ENumeric 1) (ENumeric 2) (ENumeric 3)) stream) "
                + "mapToInt (Args (ELambda (Pair def x)\n"
                + "  (SReturn (EBinary (EVariable x) + (ENumeric 1)))))) sum)))",
                  "return [1, 2, 3].stream().mapToInt((def x) -> {\n"
                + "  return x + 1\n"
                + "}).sum()");
        assertToString(
                  "(SSource (SReturn (PCallInvoke (PCallInvoke (PCallInvoke (EListInit (ENumeric 1) (ENumeric 2) (ENumeric 3)) stream) "
                + "mapToInt (Args (ELambda (Pair null x)\n"
                + "  (SReturn (EBinary (EVariable x) + (ENumeric 1)))))) sum)))",
                  "return [1, 2, 3].stream().mapToInt(x -> x + 1).sum()");
        assertToString(
                  "(SSource (SReturn (PCallInvoke (EListInit (EString 'a') (EString 'b')) sort (Args (ELambda (Pair def a) (Pair def b)\n"
                + "  (SReturn (EBinary (PCallInvoke (EVariable a) length) - (PCallInvoke (EVariable b) length))))))))",
                  "return ['a', 'b'].sort((def a, def b) -> {\n"
                + "  return a.length() - b.length()\n"
                + "})");
        assertToString(
                  "(SSource (SReturn (PCallInvoke (EListInit (EString 'a') (EString 'b')) sort (Args (ELambda (Pair null a) (Pair null b)\n"
                + "  (SReturn (EBinary (PCallInvoke (EVariable a) length) - (PCallInvoke (EVariable b) length))))))))",
                  "return ['a', 'b'].sort((a, b) -> a.length() - b.length())");
        assertToString(
                "(SSource (SReturn (PCallInvoke (EListInit (EString 'a') (EString 'b')) sort (Args (ELambda (Pair def a) (Pair def b)\n"
              + "  (SIf (EComp (EVariable a) < (EVariable b)) (SBlock "
                  + "(SReturn (EBinary (PCallInvoke (EVariable a) length) - (PCallInvoke (EVariable b) length)))))\n"
              + "  (SReturn (ENumeric 1)))))))",
                "return ['a', 'b'].sort((def a, def b) -> {\n"
              + "  if (a < b) {\n"
              + "    return a.length() - b.length()\n"
              + "  }\n"
              + "  return 1\n"
              + "})");
    }

    public void testEListInit() {
        assertToString("(SSource (SReturn (EListInit (ENumeric 1) (ENumeric 2) (EString 'cat') (EString 'dog') (ENewObj Object))))",
                "return [1, 2, 'cat', 'dog', new Object()]");
        assertToString("(SSource (SReturn (EListInit)))", "return []");
    }

    public void testEMapInit() {
        assertToString("(SSource (SReturn (EMapInit "
                    + "(Pair (EString 'a') (ENumeric 1)) "
                    + "(Pair (EString 'b') (ENumeric 3)) "
                    + "(Pair (ENumeric 12) (ENewObj Object)))))",
                "return ['a': 1, 'b': 3, 12: new Object()]");
        assertToString("(SSource (SReturn (EMapInit)))", "return [:]");
    }

    public void testENewArray() {
        assertToString("(SSource (SReturn (ENewArray int[] dims (Args (ENumeric 10)))))", "return new int[10]");
        assertToString("(SSource (SReturn (ENewArray int[][][] dims (Args (ENumeric 10) (ENumeric 4) (ENumeric 5)))))",
                "return new int[10][4][5]");
        assertToString("(SSource (SReturn (ENewArray int[] init (Args (ENumeric 1) (ENumeric 2) (ENumeric 3)))))",
                "return new int[] {1, 2, 3}");
        assertToString("(SSource (SReturn (ENewArray def[] init (Args (ENumeric 1) (ENumeric 2) (EString 'bird')))))",
                "return new def[] {1, 2, 'bird'}");
    }

    public void testENewObj() {
        assertToString("(SSource (SReturn (ENewObj Object)))", "return new Object()");
        assertToString("(SSource (SReturn (ENewObj DateTimeException (Args (EString 'test')))))", "return new DateTimeException('test')");
    }

    public void testENull() {
        assertToString("(SSource (SReturn (ENull)))", "return null");
    }

    public void testENumeric() {
        assertToString("(SSource (SReturn (ENumeric 1)))", "return 1");
        assertToString("(SSource (SReturn (ENumeric 114121d)))", "return 114121d");
        assertToString("(SSource (SReturn (ENumeric 114134f)))", "return 114134f");
        assertToString("(SSource (SReturn (ENumeric 114121D)))", "return 114121D");
        assertToString("(SSource (SReturn (ENumeric 111234F)))", "return 111234F");
        assertToString("(SSource (SReturn (ENumeric 774121l)))", "return 774121l");
        assertToString("(SSource (SReturn (ENumeric 881234L)))", "return 881234L");

        assertToString("(SSource (SReturn (ENumeric 1 16)))", "return 0x1");
        assertToString("(SSource (SReturn (ENumeric 774121l 16)))", "return 0x774121l");
        assertToString("(SSource (SReturn (ENumeric 881234L 16)))", "return 0x881234L");

        assertToString("(SSource (SReturn (ENumeric 1 8)))", "return 01");
        assertToString("(SSource (SReturn (ENumeric 774121l 8)))", "return 0774121l");
        assertToString("(SSource (SReturn (ENumeric 441234L 8)))", "return 0441234L");
    }

    public void testERegex() {
        assertToString("(SSource (SReturn (ERegex /foo/)))", "return /foo/");
        assertToString("(SSource (SReturn (ERegex /foo/ cix)))", "return /foo/cix");
        assertToString("(SSource (SReturn (ERegex /foo/ cix)))", "return /foo/xci");
    }

    public void testEStatic() {
        assertToString("(SSource (SReturn (PCallInvoke (EStatic Optional) empty)))", "return Optional.empty()");
    }

    public void testEString() {
        assertToString("(SSource (SReturn (EString 'foo')))", "return 'foo'");
        assertToString("(SSource (SReturn (EString ' oo')))", "return ' oo'");
        assertToString("(SSource (SReturn (EString 'fo ')))", "return 'fo '");
        assertToString("(SSource (SReturn (EString ' o ')))", "return ' o '");
    }

    public void testEUnary() {
        assertToString("(SSource (SReturn (EUnary ! (EBoolean true))))", "return !true");
        assertToString("(SSource (SReturn (EUnary ~ (ENumeric 1))))", "return ~1");
        assertToString("(SSource (SReturn (EUnary + (ENumeric 1))))", "return +1");
        assertToString("(SSource (SReturn (EUnary - (ENumeric 1))))", "return -(1)");
    }

    public void testEVariable() {
        assertToString("(SSource (SReturn (EVariable params)))", "return params");
        assertToString(
                  "(SSource\n"
                + "  (SDeclBlock (SDeclaration def a (ENumeric 1)))\n"
                + "  (SReturn (EVariable a)))",
                  "def a = 1;\n"
                + "return a");
    }

    public void testPBrace() {
        assertToString("(SSource (SReturn (PBrace (PField (EVariable params) a) (ENumeric 10))))", "return params.a[10]");
        assertToString("(SSource (SReturn (PBrace (EVariable params) (EString 'a'))))", "return params['a']");
    }

    public void testPCallInvoke() {
        assertToString("(SSource (SReturn (PCallInvoke (EStatic Optional) empty)))", "return Optional.empty()");
        assertToString("(SSource (SReturn (PCallInvoke (EStatic Optional) of (Args (ENumeric 1)))))", "return Optional.of(1)");
        assertToString("(SSource (SReturn (PCallInvoke (EStatic Objects) equals (Args (ENumeric 1) (ENumeric 2)))))",
                "return Objects.equals(1, 2)");
        assertToString("(SSource (SReturn (PCallInvoke (EVariable params) equals (Args (ENumeric 1)))))", "return params.equals(1)");
    }

    public void testPField() {
        assertToString("(SSource (SReturn (PField (EVariable params) a)))", "return params.a");
        assertToString("(SSource (SReturn (PField nullSafe (EVariable params) a)))", "return params?.a");
        assertToString(
                  "(SSource\n"
                + "  (SDeclBlock (SDeclaration int[] a (ENewArray int[] dims (Args (ENumeric 10)))))\n"
                + "  (SReturn (PField (EVariable a) length)))",
                  "int[] a = new int[10];\n"
                + "return a.length");
        assertToString(
                "(SSource\n"
              + "  (SDeclBlock (SDeclaration org.elasticsearch.painless.FeatureTestObject a"
              + " (ENewObj org.elasticsearch.painless.FeatureTestObject)))\n"
              + "  (SExpression (EAssignment (PField (EVariable a) x) = (ENumeric 10)))\n"
              + "  (SReturn (PField (EVariable a) x)))",
                "org.elasticsearch.painless.FeatureTestObject a = new org.elasticsearch.painless.FeatureTestObject();\n"
              + "a.x = 10;\n"
              + "return a.x");
    }

    public void testPSubArrayLength() {
        Location l = new Location(getTestName(), 0);
        PSubArrayLength node = new PSubArrayLength(l, "int", "a");
        node.prefix = new EVariable(l, "a");
        assertEquals("(PSubArrayLength (EVariable a))", node.toString());
        assertEquals("(PSubNullSafeField (PSubArrayLength (EVariable a)))", new PSubNullSafeField(l, node).toString());
    }

    public void testPSubBrace() {
        Location l = new Location(getTestName(), 0);
        PSubBrace node = new PSubBrace(l, int.class, new ENumeric(l, "1", 10));
        node.prefix = new EVariable(l, "a");
        assertEquals("(PSubBrace (EVariable a) (ENumeric 1))", node.toString());
    }

    public void testPSubCallInvoke() {
        Location l = new Location(getTestName(), 0);
        PainlessClass c = painlessLookup.lookupPainlessClass(Integer.class);
        PainlessMethod m = c.methods.get(PainlessLookupUtility.buildPainlessMethodKey("toString", 0));
        PSubCallInvoke node = new PSubCallInvoke(l, m, null, emptyList());
        node.prefix = new EVariable(l, "a");
        assertEquals("(PSubCallInvoke (EVariable a) toString)", node.toString());
        assertEquals("(PSubNullSafeCallInvoke (PSubCallInvoke (EVariable a) toString))", new PSubNullSafeCallInvoke(l, node).toString());

        l = new Location(getTestName(), 1);
        m = c.methods.get(PainlessLookupUtility.buildPainlessMethodKey("equals", 1));
        node = new PSubCallInvoke(l, m, null, singletonList(new EVariable(l, "b")));
        node.prefix = new EVariable(l, "a");
        assertEquals("(PSubCallInvoke (EVariable a) equals (Args (EVariable b)))", node.toString());
        assertEquals("(PSubNullSafeCallInvoke (PSubCallInvoke (EVariable a) equals (Args (EVariable b))))",
                new PSubNullSafeCallInvoke(l, node).toString());
    }

    public void testPSubDefArray() {
        Location l = new Location(getTestName(), 0);
        PSubDefArray node = new PSubDefArray(l, new EConstant(l, 1));
        node.prefix = new EVariable(l, "a");
        assertEquals("(PSubDefArray (EVariable a) (EConstant Integer 1))", node.toString());
    }

    public void testPSubDefCall() {
        Location l = new Location(getTestName(), 0);
        PSubDefCall node = new PSubDefCall(l, "toString", emptyList());
        node.prefix = new EVariable(l, "a");
        assertEquals("(PSubDefCall (EVariable a) toString)", node.toString());
        assertEquals("(PSubNullSafeCallInvoke (PSubDefCall (EVariable a) toString))", new PSubNullSafeCallInvoke(l, node).toString());

        l = new Location(getTestName(), 0);
        node = new PSubDefCall(l, "equals", singletonList(new EVariable(l, "b")));
        node.prefix = new EVariable(l, "a");
        assertEquals("(PSubDefCall (EVariable a) equals (Args (EVariable b)))", node.toString());
        assertEquals("(PSubNullSafeCallInvoke (PSubDefCall (EVariable a) equals (Args (EVariable b))))",
                new PSubNullSafeCallInvoke(l, node).toString());

        l = new Location(getTestName(), 0);
        node = new PSubDefCall(l, "superWeird", Arrays.asList(new EVariable(l, "b"), new EVariable(l, "c"), new EVariable(l, "d")));
        node.prefix = new EVariable(l, "a");
        assertEquals("(PSubDefCall (EVariable a) superWeird (Args (EVariable b) (EVariable c) (EVariable d)))", node.toString());
        assertEquals("(PSubNullSafeCallInvoke (PSubDefCall (EVariable a) superWeird (Args (EVariable b) (EVariable c) (EVariable d))))",
                new PSubNullSafeCallInvoke(l, node).toString());
    }

    public void testPSubDefField() {
        Location l = new Location(getTestName(), 0);
        PSubDefField node = new PSubDefField(l, "ok");
        node.prefix = new EVariable(l, "a");
        assertEquals("(PSubDefField (EVariable a) ok)", node.toString());
        assertEquals("(PSubNullSafeCallInvoke (PSubDefField (EVariable a) ok))", new PSubNullSafeCallInvoke(l, node).toString());
    }

    public void testPSubField() {
        Location l = new Location(getTestName(), 0);
        PainlessClass s = painlessLookup.lookupPainlessClass(Boolean.class);
        PainlessField f = s.staticFields.get("TRUE");
        PSubField node = new PSubField(l, f);
        node.prefix = new EStatic(l, "Boolean");
        assertEquals("(PSubField (EStatic Boolean) TRUE)", node.toString());
        assertEquals("(PSubNullSafeCallInvoke (PSubField (EStatic Boolean) TRUE))", new PSubNullSafeCallInvoke(l, node).toString());
    }

    public void testPSubListShortcut() {
        Location l = new Location(getTestName(), 0);
        PSubListShortcut node = new PSubListShortcut(l, List.class, new EConstant(l, 1));
        node.prefix = new EVariable(l, "a");
        assertEquals("(PSubListShortcut (EVariable a) (EConstant Integer 1))", node.toString());
        assertEquals("(PSubNullSafeCallInvoke (PSubListShortcut (EVariable a) (EConstant Integer 1)))",
                new PSubNullSafeCallInvoke(l, node).toString());

        l = new Location(getTestName(), 0);
        node = new PSubListShortcut(l, List.class, new EBinary(l, Operation.ADD, new EConstant(l, 1), new EConstant(l, 4)));
        node.prefix = new EVariable(l, "a");
        assertEquals("(PSubListShortcut (EVariable a) (EBinary (EConstant Integer 1) + (EConstant Integer 4)))", node.toString());
    }

    public void testPSubMapShortcut() {
        Location l = new Location(getTestName(), 0);
        PSubMapShortcut node = new PSubMapShortcut(l, Map.class, new EConstant(l, "cat"));
        node.prefix = new EVariable(l, "a");
        assertEquals("(PSubMapShortcut (EVariable a) (EConstant String 'cat'))", node.toString());
        assertEquals("(PSubNullSafeCallInvoke (PSubMapShortcut (EVariable a) (EConstant String 'cat')))",
                new PSubNullSafeCallInvoke(l, node).toString());

        l = new Location(getTestName(), 1);
        node = new PSubMapShortcut(l, Map.class, new EBinary(l, Operation.ADD, new EConstant(l, 1), new EConstant(l, 4)));
        node.prefix = new EVariable(l, "a");
        assertEquals("(PSubMapShortcut (EVariable a) (EBinary (EConstant Integer 1) + (EConstant Integer 4)))", node.toString());
    }

    public void testPSubShortcut() {
        Location l = new Location(getTestName(), 0);
        PainlessClass s = painlessLookup.lookupPainlessClass(FeatureTestObject.class);
        PainlessMethod getter = s.methods.get(PainlessLookupUtility.buildPainlessMethodKey("getX", 0));
        PainlessMethod setter = s.methods.get(PainlessLookupUtility.buildPainlessMethodKey("setX", 1));
        PSubShortcut node = new PSubShortcut(l, "x", FeatureTestObject.class.getName(), getter, setter);
        node.prefix = new EVariable(l, "a");
        assertEquals("(PSubShortcut (EVariable a) x)", node.toString());
        assertEquals("(PSubNullSafeCallInvoke (PSubShortcut (EVariable a) x))",
                new PSubNullSafeCallInvoke(l, node).toString());
    }

    public void testSBreak() {
        assertToString(
                "(SSource\n"
              + "  (SDeclBlock (SDeclaration int itr (ENumeric 2)))\n"
              + "  (SDeclBlock (SDeclaration int a (ENumeric 1)))\n"
              + "  (SDeclBlock (SDeclaration int b (ENumeric 1)))\n"
              + "  (SDo (EComp (EVariable b) < (ENumeric 1000)) (SBlock\n"
              + "    (SExpression (EAssignment (EVariable itr) ++ post))\n"
              + "    (SIf (EComp (EVariable itr) > (ENumeric 10000)) (SBlock (SBreak)))\n"
              + "    (SDeclBlock (SDeclaration int tmp (EVariable a)))\n"
              + "    (SExpression (EAssignment (EVariable a) = (EVariable b)))\n"
              + "    (SExpression (EAssignment (EVariable b) = (EBinary (EVariable tmp) + (EVariable b))))))\n"
              + "  (SReturn (EVariable b)))",
                "int itr = 2;\n"
              + "int a = 1;\n"
              + "int b = 1;\n"
              + "do {\n"
              + "  itr++;\n"
              + "  if (itr > 10000) {\n"
              + "    break\n"
              + "  }\n"
              + "  int tmp = a;\n"
              + "  a = b;\n"
              + "  b = tmp + b\n"
              + "} while (b < 1000);\n"
              + "return b");
    }

    public void testSContinue() {
        assertToString(
                "(SSource\n"
              + "  (SDeclBlock (SDeclaration int itr (ENumeric 2)))\n"
              + "  (SDeclBlock (SDeclaration int a (ENumeric 1)))\n"
              + "  (SDeclBlock (SDeclaration int b (ENumeric 1)))\n"
              + "  (SDo (EComp (EVariable b) < (ENumeric 1000)) (SBlock\n"
              + "    (SExpression (EAssignment (EVariable itr) ++ post))\n"
              + "    (SIf (EComp (EVariable itr) < (ENumeric 10000)) (SBlock (SContinue)))\n"
              + "    (SDeclBlock (SDeclaration int tmp (EVariable a)))\n"
              + "    (SExpression (EAssignment (EVariable a) = (EVariable b)))\n"
              + "    (SExpression (EAssignment (EVariable b) = (EBinary (EVariable tmp) + (EVariable b))))))\n"
              + "  (SReturn (EVariable b)))",
                "int itr = 2;\n"
              + "int a = 1;\n"
              + "int b = 1;\n"
              + "do {\n"
              + "  itr++;\n"
              + "  if (itr < 10000) {\n"
              + "    continue\n"
              + "  }\n"
              + "  int tmp = a;\n"
              + "  a = b;\n"
              + "  b = tmp + b\n"
              + "} while (b < 1000);\n"
              + "return b");
    }

    public void testSDeclBlock() {
        assertToString(
                  "(SSource\n"
                + "  (SDeclBlock (SDeclaration def a))\n"
                + "  (SExpression (EAssignment (EVariable a) = (ENumeric 10)))\n"
                + "  (SReturn (EVariable a)))",
                  "def a;\n"
                + "a = 10;\n"
                + "return a");
        assertToString(
                "(SSource\n"
              + "  (SDeclBlock (SDeclaration def a (ENumeric 10)))\n"
              + "  (SReturn (EVariable a)))",
                "def a = 10;\n"
              + "return a");
        assertToString(
                "(SSource\n"
              + "  (SDeclBlock\n"
              + "    (SDeclaration def a)\n"
              + "    (SDeclaration def b)\n"
              + "    (SDeclaration def c))\n"
              + "  (SReturn (EVariable a)))",
                "def a, b, c;\n"
              + "return a");
        assertToString(
                "(SSource\n"
              + "  (SDeclBlock\n"
              + "    (SDeclaration def a (ENumeric 10))\n"
              + "    (SDeclaration def b (ENumeric 20))\n"
              + "    (SDeclaration def c (ENumeric 100)))\n"
              + "  (SReturn (EVariable a)))",
                "def a = 10, b = 20, c = 100;\n"
              + "return a");
        assertToString(
                "(SSource\n"
              + "  (SDeclBlock\n"
              + "    (SDeclaration def a (ENumeric 10))\n"
              + "    (SDeclaration def b)\n"
              + "    (SDeclaration def c (ENumeric 100)))\n"
              + "  (SReturn (EVariable a)))",
                "def a = 10, b, c = 100;\n"
              + "return a");
        assertToString(
                "(SSource\n"
              + "  (SIf (PField (EVariable params) a) (SBlock\n"
              + "    (SDeclBlock\n"
              + "      (SDeclaration def a (ENumeric 10))\n"
              + "      (SDeclaration def b)\n"
              + "      (SDeclaration def c (ENumeric 100)))\n"
              + "    (SReturn (EVariable a))))\n"
              + "  (SReturn (EBoolean false)))",
                "if (params.a) {"
              + "  def a = 10, b, c = 100;\n"
              + "  return a\n"
              + "}\n"
              + "return false");
    }

    public void testSDo() {
        assertToString(
                  "(SSource\n"
                + "  (SDeclBlock (SDeclaration int itr (ENumeric 2)))\n"
                + "  (SDeclBlock (SDeclaration int a (ENumeric 1)))\n"
                + "  (SDeclBlock (SDeclaration int b (ENumeric 1)))\n"
                + "  (SDo (EComp (EVariable b) < (ENumeric 1000)) (SBlock\n"
                + "    (SExpression (EAssignment (EVariable itr) ++ post))\n"
                + "    (SDeclBlock (SDeclaration int tmp (EVariable a)))\n"
                + "    (SExpression (EAssignment (EVariable a) = (EVariable b)))\n"
                + "    (SExpression (EAssignment (EVariable b) = (EBinary (EVariable tmp) + (EVariable b))))))\n"
                + "  (SReturn (EVariable b)))",
                  "int itr = 2;\n"
                + "int a = 1;\n"
                + "int b = 1;\n"
                + "do {\n"
                + "  itr++;\n"
                + "  int tmp = a;\n"
                + "  a = b;\n"
                + "  b = tmp + b\n"
                + "} while (b < 1000);\n"
                + "return b");
    }

    public void testSEach() {
        assertToString(
                "(SSource\n"
              + "  (SDeclBlock (SDeclaration int l (ENumeric 0)))\n"
              + "  (SEach String s (EListInit (EString 'cat') (EString 'dog') (EString 'chicken')) (SBlock "
                  + "(SExpression (EAssignment (EVariable l) += (PCallInvoke (EVariable s) length)))))\n"
              + "  (SReturn (EVariable l)))",
                  "int l = 0;\n"
                + "for (String s : ['cat', 'dog', 'chicken']) {\n"
                + "  l += s.length()\n"
                + "}\n"
                + "return l");
        assertToString(
                "(SSource\n"
              + "  (SDeclBlock (SDeclaration int l (ENumeric 0)))\n"
              + "  (SEach String s (EListInit (EString 'cat') (EString 'dog') (EString 'chicken')) (SBlock\n"
              + "    (SDeclBlock (SDeclaration String s2 (EBinary (EString 'dire ') + (EVariable s))))\n"
              + "    (SExpression (EAssignment (EVariable l) += (PCallInvoke (EVariable s2) length)))))\n"
              + "  (SReturn (EVariable l)))",
                "int l = 0;\n"
              + "for (String s : ['cat', 'dog', 'chicken']) {\n"
              + "  String s2 = 'dire ' + s;\n"
              + "  l += s2.length()\n"
              + "}\n"
              + "return l");
    }

    public void testSFor() {
        assertToString(
                "(SSource\n"
              + "  (SDeclBlock (SDeclaration int sum (ENumeric 0)))\n"
              + "  (SFor\n"
              + "    (SDeclBlock (SDeclaration int i (ENumeric 0)))\n"
              + "    (EComp (EVariable i) < (ENumeric 1000))\n"
              + "    (EAssignment (EVariable i) ++ post)\n"
              + "    (SBlock (SExpression (EAssignment (EVariable sum) += (EVariable i)))))\n"
              + "  (SReturn (EVariable sum)))",
                  "int sum = 0;\n"
                + "for (int i = 0; i < 1000; i++) {\n"
                + "  sum += i\n"
                + "}\n"
                + "return sum");
        assertToString(
                "(SSource\n"
              + "  (SDeclBlock (SDeclaration int sum (ENumeric 0)))\n"
              + "  (SFor\n"
              + "    (SDeclBlock (SDeclaration int i (ENumeric 0)))\n"
              + "    (EComp (EVariable i) < (ENumeric 1000))\n"
              + "    (EAssignment (EVariable i) ++ post)\n"
              + "    (SBlock (SFor\n"
              + "      (SDeclBlock (SDeclaration int j (ENumeric 0)))\n"
              + "      (EComp (EVariable j) < (ENumeric 1000))\n"
              + "      (EAssignment (EVariable j) ++ post)\n"
              + "      (SBlock (SExpression (EAssignment (EVariable sum) += (EBinary (EVariable i) * (EVariable j))))))))\n"
              + "  (SReturn (EVariable sum)))",
                "int sum = 0;\n"
              + "for (int i = 0; i < 1000; i++) {\n"
              + "  for (int j = 0; j < 1000; j++) {\n"
              + "    sum += i * j\n"
              + "  }\n"
              + "}\n"
              + "return sum");
    }

    public void testSIf() {
        assertToString(
                "(SSource (SIf (PField (EVariable param) a) (SBlock (SReturn (EBoolean true)))))",
                  "if (param.a) {\n"
                + "  return true\n"
                +"}");
        assertToString(
                "(SSource (SIf (PField (EVariable param) a) (SBlock\n"
              + "  (SIf (PField (EVariable param) b) (SBlock (SReturn (EBoolean true))))\n"
              + "  (SReturn (EBoolean false)))))",
                "if (param.a) {\n"
              + "  if (param.b) {\n"
              + "    return true\n"
              + "  }\n"
              + "  return false\n"
              +"}");
    }

    public void testSIfElse() {
        assertToString(
                  "(SSource (SIfElse (PField (EVariable param) a)\n"
                + "  (SBlock (SReturn (EBoolean true)))\n"
                + "  (SBlock (SReturn (EBoolean false)))))",
                  "if (param.a) {\n"
                + "  return true\n"
                + "} else {\n"
                + "  return false\n"
                + "}");
        assertToString(
                "(SSource\n"
              + "  (SDeclBlock (SDeclaration int i (ENumeric 0)))\n"
              + "  (SIfElse (PField (EVariable param) a)\n"
              + "    (SBlock (SIfElse (PField (EVariable param) b)\n"
              + "      (SBlock (SReturn (EBoolean true)))\n"
              + "      (SBlock (SReturn (EString 'cat')))))\n"
              + "    (SBlock (SReturn (EBoolean false)))))",
                "int i = 0;\n"
              + "if (param.a) {\n"
              + "  if (param.b) {\n"
              + "    return true\n"
              + "  } else {\n"
              + "    return 'cat'\n"
              + "  }\n"
              + "} else {"
              + "  return false\n"
              + "}");
    }

    public void testSSubEachArray() {
        Location l = new Location(getTestName(), 0);
        Variable v = new Variable(l, "test", int.class, 5, false);
        AExpression e = new ENewArray(l, "int", Arrays.asList(new EConstant(l, 1), new EConstant(l, 2), new EConstant(l, 3)), true);
        SBlock b = new SBlock(l, singletonList(new SReturn(l, new EConstant(l, 5))));
        SSubEachArray node = new SSubEachArray(l, v, e, b);
        assertEquals(
                "(SSubEachArray int test (ENewArray int init (Args (EConstant Integer 1) (EConstant Integer 2) (EConstant Integer 3))) "
              + "(SBlock (SReturn (EConstant Integer 5))))",
                node.toString());
    }

    public void testSSubEachIterable() {
        Location l = new Location(getTestName(), 0);
        Variable v = new Variable(l, "test", int.class, 5, false);
        AExpression e = new EListInit(l, Arrays.asList(new EConstant(l, 1), new EConstant(l, 2), new EConstant(l, 3)));
        SBlock b = new SBlock(l, singletonList(new SReturn(l, new EConstant(l, 5))));
        SSubEachIterable node = new SSubEachIterable(l, v, e, b);
        assertEquals(
                  "(SSubEachIterable int test (EListInit (EConstant Integer 1) (EConstant Integer 2) (EConstant Integer 3)) (SBlock "
                + "(SReturn (EConstant Integer 5))))",
                  node.toString());
    }

    public void testSThrow() {
        assertToString("(SSource (SThrow (ENewObj RuntimeException)))", "throw new RuntimeException()");
    }

    public void testSWhile() {
        assertToString(
                  "(SSource\n"
                + "  (SDeclBlock (SDeclaration int i (ENumeric 0)))\n"
                + "  (SWhile (EComp (EVariable i) < (ENumeric 10)) (SBlock (SExpression (EAssignment (EVariable i) ++ post))))\n"
                + "  (SReturn (EVariable i)))",
                  "int i = 0;\n"
                + "while (i < 10) {\n"
                + "  i++\n"
                + "}\n"
                + "return i");
    }

    public void testSFunction() {
        assertToString(
                  "(SSource\n"
                + "  (SFunction def a\n"
                + "    (SReturn (EBoolean true)))\n"
                + "  (SReturn (EBoolean true)))",
                  "def a() {\n"
                + "  return true\n"
                + "}\n"
                + "return true");
        assertToString(
                "(SSource\n"
              + "  (SFunction def a (Args (Pair int i) (Pair int j))\n"
              + "    (SReturn (EBoolean true)))\n"
              + "  (SReturn (EBoolean true)))",
                "def a(int i, int j) {\n"
              + "  return true\n"
              + "}\n"
              + "return true");
        assertToString(
                "(SSource\n"
              + "  (SFunction def a (Args (Pair int i) (Pair int j))\n"
              + "    (SIf (EComp (EVariable i) < (EVariable j)) (SBlock (SReturn (EBoolean true))))\n"
              + "    (SDeclBlock (SDeclaration int k (EBinary (EVariable i) + (EVariable j))))\n"
              + "    (SReturn (EVariable k)))\n"
              + "  (SReturn (EBoolean true)))",
                "def a(int i, int j) {\n"
              + "  if (i < j) {\n"
              + "    return true\n"
              + "  }\n"
              + "  int k = i + j;\n"
              + "  return k\n"
              + "}\n"
              + "return true");
        assertToString(
                "(SSource\n"
              + "  (SFunction def a\n"
              + "    (SReturn (EBoolean true)))\n"
              + "  (SFunction def b\n"
              + "    (SReturn (EBoolean false)))\n"
              + "  (SReturn (EBoolean true)))",
                "def a() {\n"
              + "  return true\n"
              + "}\n"
              + "def b() {\n"
              + "  return false\n"
              + "}\n"
              + "return true");
    }

    public void testSTryAndSCatch() {
        assertToString(
                  "(SSource (STry (SBlock (SReturn (ENumeric 1)))\n"
                + "  (SCatch Exception e (SBlock (SReturn (ENumeric 2))))))",
                  "try {\n"
                + "  return 1\n"
                + "} catch (Exception e) {\n"
                + "  return 2\n"
                + "}");
        assertToString(
                "(SSource (STry (SBlock\n"
              + "  (SDeclBlock (SDeclaration int i (ENumeric 1)))\n"
              + "  (SReturn (ENumeric 1)))\n"
              + "  (SCatch Exception e (SBlock (SReturn (ENumeric 2))))))",
                "try {\n"
              + "  int i = 1;"
              + "  return 1\n"
              + "} catch (Exception e) {\n"
              + "  return 2\n"
              + "}");
        assertToString(
                "(SSource (STry (SBlock (SReturn (ENumeric 1)))\n"
              + "  (SCatch Exception e (SBlock\n"
              + "    (SDeclBlock (SDeclaration int i (ENumeric 1)))\n"
              + "    (SReturn (ENumeric 2))))))",
                "try {\n"
              + "  return 1\n"
              + "} catch (Exception e) {"
              + "  int i = 1;\n"
              + "  return 2\n"
              + "}");
        assertToString(
                "(SSource (STry (SBlock (SReturn (ENumeric 1)))\n"
              + "  (SCatch NullPointerException e (SBlock (SReturn (ENumeric 2))))\n"
              + "  (SCatch Exception e (SBlock (SReturn (ENumeric 3))))))",
                "try {\n"
              + "  return 1\n"
              + "} catch (NullPointerException e) {\n"
              + "  return 2\n"
              + "} catch (Exception e) {\n"
              + "  return 3\n"
              + "}");
    }

    private final PainlessLookup painlessLookup;

    public NodeToStringTests() {
        List<Whitelist> whitelists = new ArrayList<>(Whitelist.BASE_WHITELISTS);
        whitelists.add(WhitelistLoader.loadFromResourceFiles(Whitelist.class, "org.elasticsearch.painless.test"));
        painlessLookup = PainlessLookupBuilder.buildFromWhitelists(whitelists);
    }

    private void assertToString(String expected, String code) {
        assertEquals(expected, walk(code).toString());
    }

    private SSource walk(String code) {
        ScriptClassInfo scriptClassInfo = new ScriptClassInfo(painlessLookup, PainlessTestScript.class);
        CompilerSettings compilerSettings = new CompilerSettings();
        compilerSettings.setRegexesEnabled(true);
        try {
            return Walker.buildPainlessTree(
                scriptClassInfo, getTestName(), code, compilerSettings, painlessLookup, null);
        } catch (Exception e) {
            throw new AssertionError("Failed to compile: " + code, e);
        }
    }
}
