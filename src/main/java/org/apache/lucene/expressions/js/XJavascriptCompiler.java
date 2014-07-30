package org.apache.lucene.expressions.js;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.Tree;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.queries.function.FunctionValues;
import org.apache.lucene.util.IOUtils;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.GeneratorAdapter;

/**
 * An expression compiler for javascript expressions.
 * <p>
 * Example:
 * <pre class="prettyprint">
 *   Expression foo = XJavascriptCompiler.compile("((0.3*popularity)/10.0)+(0.7*score)");
 * </pre>
 * <p>
 * See the {@link org.apache.lucene.expressions.js package documentation} for 
 * the supported syntax and default functions.
 * <p>
 * You can compile with an alternate set of functions via {@link #compile(String, Map, ClassLoader)}.
 * For example:
 * <pre class="prettyprint">
 *   Map&lt;String,Method&gt; functions = new HashMap&lt;&gt;();
 *   // add all the default functions
 *   functions.putAll(XJavascriptCompiler.DEFAULT_FUNCTIONS);
 *   // add cbrt()
 *   functions.put("cbrt", Math.class.getMethod("cbrt", double.class));
 *   // call compile with customized function map
 *   Expression foo = XJavascriptCompiler.compile("cbrt(score)+ln(popularity)", 
 *                                               functions, 
 *                                               getClass().getClassLoader());
 * </pre>
 *
 * @lucene.experimental
 */
public class XJavascriptCompiler {

    static {
        assert org.elasticsearch.Version.CURRENT.luceneVersion == org.apache.lucene.util.Version.LUCENE_4_9: "Remove this code once we upgrade to Lucene 4.10 (LUCENE-5806)";
    }

    static final class Loader extends ClassLoader {
        Loader(ClassLoader parent) {
            super(parent);
        }

        public Class<? extends Expression> define(String className, byte[] bytecode) {
            return defineClass(className, bytecode, 0, bytecode.length).asSubclass(Expression.class);
        }
    }

    private static final int CLASSFILE_VERSION = Opcodes.V1_7;

    // We use the same class name for all generated classes as they all have their own class loader.
    // The source code is displayed as "source file name" in stack trace.
    private static final String COMPILED_EXPRESSION_CLASS = XJavascriptCompiler.class.getName() + "$CompiledExpression";
    private static final String COMPILED_EXPRESSION_INTERNAL = COMPILED_EXPRESSION_CLASS.replace('.', '/');

    private static final Type EXPRESSION_TYPE = Type.getType(Expression.class);
    private static final Type FUNCTION_VALUES_TYPE = Type.getType(FunctionValues.class);

    private static final org.objectweb.asm.commons.Method
            EXPRESSION_CTOR = getMethod("void <init>(String, String[])"),
            EVALUATE_METHOD = getMethod("double evaluate(int, " + FunctionValues.class.getName() + "[])"),
            DOUBLE_VAL_METHOD = getMethod("double doubleVal(int)");

    // to work around import clash:
    private static org.objectweb.asm.commons.Method getMethod(String method) {
        return org.objectweb.asm.commons.Method.getMethod(method);
    }

    // This maximum length is theoretically 65535 bytes, but as its CESU-8 encoded we dont know how large it is in bytes, so be safe
    // rcmuir: "If your ranking function is that large you need to check yourself into a mental institution!"
    private static final int MAX_SOURCE_LENGTH = 16384;

    private final String sourceText;
    private final Map<String, Integer> externalsMap = new LinkedHashMap<>();
    private final ClassWriter classWriter = new ClassWriter(ClassWriter.COMPUTE_FRAMES | ClassWriter.COMPUTE_MAXS);
    private GeneratorAdapter gen;

    private final Map<String,Method> functions;

    /**
     * Compiles the given expression.
     *
     * @param sourceText The expression to compile
     * @return A new compiled expression
     * @throws ParseException on failure to compile
     */
    public static Expression compile(String sourceText) throws ParseException {
        return new XJavascriptCompiler(sourceText).compileExpression(XJavascriptCompiler.class.getClassLoader());
    }

    /**
     * Compiles the given expression with the supplied custom functions.
     * <p>
     * Functions must be {@code public static}, return {@code double} and 
     * can take from zero to 256 {@code double} parameters.
     *
     * @param sourceText The expression to compile
     * @param functions map of String names to functions
     * @param parent a {@code ClassLoader} that should be used as the parent of the loaded class.
     *   It must contain all classes referred to by the given {@code functions}.
     * @return A new compiled expression
     * @throws ParseException on failure to compile
     */
    public static Expression compile(String sourceText, Map<String,Method> functions, ClassLoader parent) throws ParseException {
        if (parent == null) {
            throw new NullPointerException("A parent ClassLoader must be given.");
        }
        for (Method m : functions.values()) {
            checkFunction(m, parent);
        }
        return new XJavascriptCompiler(sourceText, functions).compileExpression(parent);
    }

    /**
     * This method is unused, it is just here to make sure that the function signatures don't change.
     * If this method fails to compile, you also have to change the byte code generator to correctly
     * use the FunctionValues class.
     */
    @SuppressWarnings({"unused", "null"})
    private static void unusedTestCompile() {
        FunctionValues f = null;
        double ret = f.doubleVal(2);
    }

    /**
     * Constructs a compiler for expressions.
     * @param sourceText The expression to compile
     */
    private XJavascriptCompiler(String sourceText) {
        this(sourceText, DEFAULT_FUNCTIONS);
    }

    /**
     * Constructs a compiler for expressions with specific set of functions
     * @param sourceText The expression to compile
     */
    private XJavascriptCompiler(String sourceText, Map<String,Method> functions) {
        if (sourceText == null) {
            throw new NullPointerException();
        }
        this.sourceText = sourceText;
        this.functions = functions;
    }

    /**
     * Compiles the given expression with the specified parent classloader
     *
     * @return A new compiled expression
     * @throws ParseException on failure to compile
     */
    private Expression compileExpression(ClassLoader parent) throws ParseException {
        try {
            Tree antlrTree = getAntlrComputedExpressionTree();

            beginCompile();
            recursiveCompile(antlrTree, Type.DOUBLE_TYPE);
            endCompile();

            Class<? extends Expression> evaluatorClass = new Loader(parent)
                    .define(COMPILED_EXPRESSION_CLASS, classWriter.toByteArray());
            Constructor<? extends Expression> constructor = evaluatorClass.getConstructor(String.class, String[].class);
            return constructor.newInstance(sourceText, externalsMap.keySet().toArray(new String[externalsMap.size()]));
        } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException exception) {
            throw new IllegalStateException("An internal error occurred attempting to compile the expression (" + sourceText + ").", exception);
        }
    }

    private void beginCompile() {
        classWriter.visit(CLASSFILE_VERSION,
                Opcodes.ACC_PUBLIC | Opcodes.ACC_SUPER | Opcodes.ACC_FINAL | Opcodes.ACC_SYNTHETIC,
                COMPILED_EXPRESSION_INTERNAL,
                null, EXPRESSION_TYPE.getInternalName(), null);
        String clippedSourceText = (sourceText.length() <= MAX_SOURCE_LENGTH) ?
                sourceText : (sourceText.substring(0, MAX_SOURCE_LENGTH - 3) + "...");
        classWriter.visitSource(clippedSourceText, null);

        GeneratorAdapter constructor = new GeneratorAdapter(Opcodes.ACC_PUBLIC | Opcodes.ACC_SYNTHETIC,
                EXPRESSION_CTOR, null, null, classWriter);
        constructor.loadThis();
        constructor.loadArgs();
        constructor.invokeConstructor(EXPRESSION_TYPE, EXPRESSION_CTOR);
        constructor.returnValue();
        constructor.endMethod();

        gen = new GeneratorAdapter(Opcodes.ACC_PUBLIC | Opcodes.ACC_SYNTHETIC,
                EVALUATE_METHOD, null, null, classWriter);
    }

    private void recursiveCompile(Tree current, Type expected) {
        int type = current.getType();
        String text = current.getText();

        switch (type) {
            case XJavascriptParser.AT_CALL:
                Tree identifier = current.getChild(0);
                String call = identifier.getText();
                int arguments = current.getChildCount() - 1;

                Method method = functions.get(call);
                if (method == null) {
                    throw new IllegalArgumentException("Unrecognized method call (" + call + ").");
                }

                int arity = method.getParameterTypes().length;
                if (arguments != arity) {
                    throw new IllegalArgumentException("Expected (" + arity + ") arguments for method call (" +
                            call + "), but found (" + arguments + ").");
                }

                for (int argument = 1; argument <= arguments; ++argument) {
                    recursiveCompile(current.getChild(argument), Type.DOUBLE_TYPE);
                }

                gen.invokeStatic(Type.getType(method.getDeclaringClass()),
                        org.objectweb.asm.commons.Method.getMethod(method));

                gen.cast(Type.DOUBLE_TYPE, expected);
                break;
            case XJavascriptParser.VARIABLE:
                int index;

                // normalize quotes
                text = normalizeQuotes(text);


                if (externalsMap.containsKey(text)) {
                    index = externalsMap.get(text);
                } else {
                    index = externalsMap.size();
                    externalsMap.put(text, index);
                }

                gen.loadArg(1);
                gen.push(index);
                gen.arrayLoad(FUNCTION_VALUES_TYPE);
                gen.loadArg(0);
                gen.invokeVirtual(FUNCTION_VALUES_TYPE, DOUBLE_VAL_METHOD);
                gen.cast(Type.DOUBLE_TYPE, expected);
                break;
            case XJavascriptParser.HEX:
                pushLong(expected, Long.parseLong(text.substring(2), 16));
                break;
            case XJavascriptParser.OCTAL:
                pushLong(expected, Long.parseLong(text.substring(1), 8));
                break;
            case XJavascriptParser.DECIMAL:
                gen.push(Double.parseDouble(text));
                gen.cast(Type.DOUBLE_TYPE, expected);
                break;
            case XJavascriptParser.AT_NEGATE:
                recursiveCompile(current.getChild(0), Type.DOUBLE_TYPE);
                gen.visitInsn(Opcodes.DNEG);
                gen.cast(Type.DOUBLE_TYPE, expected);
                break;
            case XJavascriptParser.AT_ADD:
                pushArith(Opcodes.DADD, current, expected);
                break;
            case XJavascriptParser.AT_SUBTRACT:
                pushArith(Opcodes.DSUB, current, expected);
                break;
            case XJavascriptParser.AT_MULTIPLY:
                pushArith(Opcodes.DMUL, current, expected);
                break;
            case XJavascriptParser.AT_DIVIDE:
                pushArith(Opcodes.DDIV, current, expected);
                break;
            case XJavascriptParser.AT_MODULO:
                pushArith(Opcodes.DREM, current, expected);
                break;
            case XJavascriptParser.AT_BIT_SHL:
                pushShift(Opcodes.LSHL, current, expected);
                break;
            case XJavascriptParser.AT_BIT_SHR:
                pushShift(Opcodes.LSHR, current, expected);
                break;
            case XJavascriptParser.AT_BIT_SHU:
                pushShift(Opcodes.LUSHR, current, expected);
                break;
            case XJavascriptParser.AT_BIT_AND:
                pushBitwise(Opcodes.LAND, current, expected);
                break;
            case XJavascriptParser.AT_BIT_OR:
                pushBitwise(Opcodes.LOR, current, expected);
                break;
            case XJavascriptParser.AT_BIT_XOR:
                pushBitwise(Opcodes.LXOR, current, expected);
                break;
            case XJavascriptParser.AT_BIT_NOT:
                recursiveCompile(current.getChild(0), Type.LONG_TYPE);
                gen.push(-1L);
                gen.visitInsn(Opcodes.LXOR);
                gen.cast(Type.LONG_TYPE, expected);
                break;
            case XJavascriptParser.AT_COMP_EQ:
                pushCond(GeneratorAdapter.EQ, current, expected);
                break;
            case XJavascriptParser.AT_COMP_NEQ:
                pushCond(GeneratorAdapter.NE, current, expected);
                break;
            case XJavascriptParser.AT_COMP_LT:
                pushCond(GeneratorAdapter.LT, current, expected);
                break;
            case XJavascriptParser.AT_COMP_GT:
                pushCond(GeneratorAdapter.GT, current, expected);
                break;
            case XJavascriptParser.AT_COMP_LTE:
                pushCond(GeneratorAdapter.LE, current, expected);
                break;
            case XJavascriptParser.AT_COMP_GTE:
                pushCond(GeneratorAdapter.GE, current, expected);
                break;
            case XJavascriptParser.AT_BOOL_NOT:
                Label labelNotTrue = new Label();
                Label labelNotReturn = new Label();

                recursiveCompile(current.getChild(0), Type.INT_TYPE);
                gen.visitJumpInsn(Opcodes.IFEQ, labelNotTrue);
                pushBoolean(expected, false);
                gen.goTo(labelNotReturn);
                gen.visitLabel(labelNotTrue);
                pushBoolean(expected, true);
                gen.visitLabel(labelNotReturn);
                break;
            case XJavascriptParser.AT_BOOL_AND:
                Label andFalse = new Label();
                Label andEnd = new Label();

                recursiveCompile(current.getChild(0), Type.INT_TYPE);
                gen.visitJumpInsn(Opcodes.IFEQ, andFalse);
                recursiveCompile(current.getChild(1), Type.INT_TYPE);
                gen.visitJumpInsn(Opcodes.IFEQ, andFalse);
                pushBoolean(expected, true);
                gen.goTo(andEnd);
                gen.visitLabel(andFalse);
                pushBoolean(expected, false);
                gen.visitLabel(andEnd);
                break;
            case XJavascriptParser.AT_BOOL_OR:
                Label orTrue = new Label();
                Label orEnd = new Label();

                recursiveCompile(current.getChild(0), Type.INT_TYPE);
                gen.visitJumpInsn(Opcodes.IFNE, orTrue);
                recursiveCompile(current.getChild(1), Type.INT_TYPE);
                gen.visitJumpInsn(Opcodes.IFNE, orTrue);
                pushBoolean(expected, false);
                gen.goTo(orEnd);
                gen.visitLabel(orTrue);
                pushBoolean(expected, true);
                gen.visitLabel(orEnd);
                break;
            case XJavascriptParser.AT_COND_QUE:
                Label condFalse = new Label();
                Label condEnd = new Label();

                recursiveCompile(current.getChild(0), Type.INT_TYPE);
                gen.visitJumpInsn(Opcodes.IFEQ, condFalse);
                recursiveCompile(current.getChild(1), expected);
                gen.goTo(condEnd);
                gen.visitLabel(condFalse);
                recursiveCompile(current.getChild(2), expected);
                gen.visitLabel(condEnd);
                break;
            default:
                throw new IllegalStateException("Unknown operation specified: (" + current.getText() + ").");
        }
    }

    private void pushArith(int operator, Tree current, Type expected) {
        pushBinaryOp(operator, current, expected, Type.DOUBLE_TYPE, Type.DOUBLE_TYPE, Type.DOUBLE_TYPE);
    }

    private void pushShift(int operator, Tree current, Type expected) {
        pushBinaryOp(operator, current, expected, Type.LONG_TYPE, Type.INT_TYPE, Type.LONG_TYPE);
    }

    private void pushBitwise(int operator, Tree current, Type expected) {
        pushBinaryOp(operator, current, expected, Type.LONG_TYPE, Type.LONG_TYPE, Type.LONG_TYPE);
    }

    private void pushBinaryOp(int operator, Tree current, Type expected, Type arg1, Type arg2, Type returnType) {
        recursiveCompile(current.getChild(0), arg1);
        recursiveCompile(current.getChild(1), arg2);
        gen.visitInsn(operator);
        gen.cast(returnType, expected);
    }

    private void pushCond(int operator, Tree current, Type expected) {
        Label labelTrue = new Label();
        Label labelReturn = new Label();

        recursiveCompile(current.getChild(0), Type.DOUBLE_TYPE);
        recursiveCompile(current.getChild(1), Type.DOUBLE_TYPE);

        gen.ifCmp(Type.DOUBLE_TYPE, operator, labelTrue);
        pushBoolean(expected, false);
        gen.goTo(labelReturn);
        gen.visitLabel(labelTrue);
        pushBoolean(expected, true);
        gen.visitLabel(labelReturn);
    }

    private void pushBoolean(Type expected, boolean truth) {
        switch (expected.getSort()) {
            case Type.INT:
                gen.push(truth);
                break;
            case Type.LONG:
                gen.push(truth ? 1L : 0L);
                break;
            case Type.DOUBLE:
                gen.push(truth ? 1. : 0.);
                break;
            default:
                throw new IllegalStateException("Invalid expected type: " + expected);
        }
    }

    private void pushLong(Type expected, long i) {
        switch (expected.getSort()) {
            case Type.INT:
                gen.push((int) i);
                break;
            case Type.LONG:
                gen.push(i);
                break;
            case Type.DOUBLE:
                gen.push((double) i);
                break;
            default:
                throw new IllegalStateException("Invalid expected type: " + expected);
        }
    }

    private void endCompile() {
        gen.returnValue();
        gen.endMethod();

        classWriter.visitEnd();
    }

    private Tree getAntlrComputedExpressionTree() throws ParseException {
        CharStream input = new ANTLRStringStream(sourceText);
        XJavascriptLexer lexer = new XJavascriptLexer(input);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        XJavascriptParser parser = new XJavascriptParser(tokens);

        try {
            return parser.expression().tree;

        } catch (RecognitionException exception) {
            throw new IllegalArgumentException(exception);
        } catch (RuntimeException exception) {
            if (exception.getCause() instanceof ParseException) {
                throw (ParseException)exception.getCause();
            }
            throw exception;
        }
    }

    private static String normalizeQuotes(String text) {
        StringBuilder out = new StringBuilder(text.length());
        boolean inDoubleQuotes = false;
        for (int i = 0; i < text.length(); ++i) {
            char c = text.charAt(i);
            if (c == '\\') {
                c = text.charAt(++i);
                if (c == '\\') {
                    out.append('\\'); // re-escape the backslash
                }
                // no escape for double quote
            } else if (c == '\'') {
                if (inDoubleQuotes) {
                    // escape in output
                    out.append('\\');
                } else {
                    int j = findSingleQuoteStringEnd(text, i);
                    out.append(text, i, j); // copy up to end quote (leave end for append below)
                    i = j;
                }
            } else if (c == '"') {
                c = '\''; // change beginning/ending doubles to singles
                inDoubleQuotes = !inDoubleQuotes;
            }
            out.append(c);
        }
        return out.toString();
    }

    private static int findSingleQuoteStringEnd(String text, int start) {
        ++start; // skip beginning
        while (text.charAt(start) != '\'') {
            if (text.charAt(start) == '\\') {
                ++start; // blindly consume escape value
            }
            ++start;
        }
        return start;
    }

    /**
     * The default set of functions available to expressions.
     * <p>
     * See the {@link org.apache.lucene.expressions.js package documentation}
     * for a list.
     */
    public static final Map<String,Method> DEFAULT_FUNCTIONS;
    static {
        Map<String,Method> map = new HashMap<>();
        try {
            final Properties props = new Properties();
            try (Reader in = IOUtils.getDecodingReader(JavascriptCompiler.class,
                    JavascriptCompiler.class.getSimpleName() + ".properties", StandardCharsets.UTF_8)) {
                props.load(in);
            }
            for (final String call : props.stringPropertyNames()) {
                final String[] vals = props.getProperty(call).split(",");
                if (vals.length != 3) {
                    throw new Error("Syntax error while reading Javascript functions from resource");
                }
                final Class<?> clazz = Class.forName(vals[0].trim());
                final String methodName = vals[1].trim();
                final int arity = Integer.parseInt(vals[2].trim());
                @SuppressWarnings({"rawtypes", "unchecked"}) Class[] args = new Class[arity];
                Arrays.fill(args, double.class);
                Method method = clazz.getMethod(methodName, args);
                checkFunction(method, JavascriptCompiler.class.getClassLoader());
                map.put(call, method);
            }
        } catch (NoSuchMethodException | ClassNotFoundException | IOException e) {
            throw new Error("Cannot resolve function", e);
        }
        DEFAULT_FUNCTIONS = Collections.unmodifiableMap(map);
    }

    private static void checkFunction(Method method, ClassLoader parent) {
        // We can only call the function if the given parent class loader of our compiled class has access to the method:
        final ClassLoader functionClassloader = method.getDeclaringClass().getClassLoader();
        if (functionClassloader != null) { // it is a system class iff null!
            boolean found = false;
            while (parent != null) {
                if (parent == functionClassloader) {
                    found = true;
                    break;
                }
                parent = parent.getParent();
            }
            if (!found) {
                throw new IllegalArgumentException(method + " is not declared by a class which is accessible by the given parent ClassLoader.");
            }
        }
        // do some checks if the signature is "compatible":
        if (!Modifier.isStatic(method.getModifiers())) {
            throw new IllegalArgumentException(method + " is not static.");
        }
        if (!Modifier.isPublic(method.getModifiers())) {
            throw new IllegalArgumentException(method + " is not public.");
        }
        if (!Modifier.isPublic(method.getDeclaringClass().getModifiers())) {
            throw new IllegalArgumentException(method.getDeclaringClass().getName() + " is not public.");
        }
        for (Class<?> clazz : method.getParameterTypes()) {
            if (!clazz.equals(double.class)) {
                throw new IllegalArgumentException(method + " must take only double parameters");
            }
        }
        if (method.getReturnType() != double.class) {
            throw new IllegalArgumentException(method + " does not return a double.");
        }
    }
}

