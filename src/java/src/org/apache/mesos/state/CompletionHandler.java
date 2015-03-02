package org.apache.mesos.state;

public interface CompletionHandler<T> {
  void call(T obj);
}
