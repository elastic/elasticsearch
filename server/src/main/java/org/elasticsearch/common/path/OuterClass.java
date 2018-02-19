package org.elasticsearch.common.path;

public class OuterClass<T> {

	private T data;

	private OuterClass(T in) {
		this.data = in;
	}

	public T getData() {
		return this.data;
	}

	public static class Builder<T> {

		private T last;

		@SuppressWarnings("rawtypes")
		public Builder add(T in) {
			this.last = in;
			return this;
		}

		public OuterClass<T> build() {
			return new OuterClass<T>(this.last);
		}
	}

}
