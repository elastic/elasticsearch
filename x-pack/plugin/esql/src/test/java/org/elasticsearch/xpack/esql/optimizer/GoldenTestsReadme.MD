# Golden tests

## What are golden tests?

Golden tests, or [characterization tests](https://en.wikipedia.org/wiki/Characterization_test), are
tests which record (or snapshot) the current behavior of a system, and then verify the behavior has
not changed.
Contrast with regular (unit) tests, which include a specific assertion of some computed value.

## How do I add a new golden test?

`extend` `org.elasticsearch.xpack.esql.optimizer.GoldenTestCase`, and add new tests by invoking the
`runGoldenTest` method.

```java
class MyGoldenTest extends GoldenTestCase {
  public void testBasicFrom() {
    runGoldenTest("FROM employees");
  }
}
```

The test runner will pick up the "orphan" test and create the required outputs (see next section)
based on the test name.
You can also specify which plans you want to verify (all plans are output by default):

```java
public void testBasic() throws Exception {
  runGoldenTest("FROM employees", EnumSet.of(Stage.ANALYZER, Stage.LOGICAL));
}
```

Golden test outputs are stored in a resource test directory depending on the concrete test class and
the test method name, for example,
`/path/golden_tests/my_golden_test/test_basic_from/physical.expected`.
By default, all test methods create a single directory per method, but you can also have further
directory nesting.

```java
public void testNested() throws Exception {
  for (String type : List.of("int", "long", "double")) {
    runGoldenTest(
        "FROM employees | eval emp_no = emp_no :: " + type,
        EnumSet.allOf(Stage.class),
        type); // Creates a nested directory per type.
  }
}
```

You can also use a fluent builder to specify the test options if you wish:

```java
public void testFluent() throws Exception {
  for (String type : List.of("int", "long", "double")) {
    builder("FROM employees | eval emp_no = emp_no :: " + type)
        .nestedPath(type)
        .searchStats(getSearchStats())
        .run();
  }
}
```

## What happens when a golden test fails?

When a golden test fails, the test runner will create a new `.actual` file, in the same directory as
the `.expected` file. `.actual` files are git ignored by default, you can manually delete them after
inspection, or if you wish to avoid the `.actual` file creation, you can use `-Dgolden.noactual`.
If you want to automatically delete them on test success, you can use `-Dgolden.cleanactuals`.

## How do I bulldoze an existing output?

Of course, sometimes you *want* the output to change.
Use `-Dgolden.bulldoze` to **bulldoze** (or overwrite) all the existing outputs.
You can use this in combination with `-Dtests.method` to bulldoze specific tests.
This can also be used in combination with `git diff` to inspect the differences if you prefer that
method to comparing `.actual` and `.expected` files.

## Drawbacks of golden tests

Golden tests are by their very nature much noisier than regular tests:
They compare the entire output of the system, rather than a specific value.
For example, you might change the name of a local variable in the logical plan, only to change it
back later in the physical plan.
This change has no effect on the final outcome, nor does it incur any runtime cost, but it will
cause the golden test to fail nonetheless if you are verifying that the logical plan hasn't changed.

One should feel comfortable bulldozing failed tests if one expects changes in the underlying tree.
Golden tests should be the *first* line of defense, not the only one!
