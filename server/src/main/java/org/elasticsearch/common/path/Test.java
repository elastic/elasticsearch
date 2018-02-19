package org.elasticsearch.common.path;

public class Test {

	@SuppressWarnings("unchecked")
	public static void main(String [] args) {
		// We can not instantiate the outer class since the constructor is private
		// OuterClass<String> s = new OuterClass<String>("World");

		// Instead we must use the builder
		example.OuterClass.Builder<String> builder = new OuterClass.Builder<String>();

		builder.add("Hello")
		.add("again")
		.add("World");

		OuterClass<String> oc = builder.build();

		System.out.println(oc.getData());
  }
}
