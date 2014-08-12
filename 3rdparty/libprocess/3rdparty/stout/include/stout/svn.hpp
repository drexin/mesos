/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef __STOUT_SVN_HPP__
#define __STOUT_SVN_HPP__

#include <apr_pools.h>

#include <svn_delta.h>
#include <svn_error.h>
#include <svn_pools.h>

#include <string>

#include <stout/try.hpp>

namespace svn {

struct Diff
{
  Diff(const std::string& data) : data(data) {}

  std::string data;
};


inline Try<Diff> diff(const std::string& from, const std::string& to)
{
  apr_initialize();

  apr_pool_t* pool = svn_pool_create(NULL);

  svn_txdelta_window_handler_t handler;
  void* baton = NULL;

  // Set up a pipeline for constructing an SVN diff:
  //  (1) source to target delta ---> (2) svndiff

  // The pipeline gets set up in reverse, the last stage to the first.

  svn_stringbuf_t* stringbuf = svn_stringbuf_create_ensure(1024, pool);

  svn_txdelta_to_svndiff2(
      &handler,
      &baton,
      svn_stream_from_stringbuf(stringbuf, pool),
      0,
      pool);

  svn_string_t source;
  source.data = from.data();
  source.len = from.length();

  svn_string_t target;
  target.data = to.data();
  target.len = to.length();

  svn_txdelta_stream_t* input;
  svn_txdelta(
      &input,
      svn_stream_from_string(&source, pool),
      svn_stream_from_string(&target, pool),
      pool);

  svn_error_t* error = svn_txdelta_send_txstream(input, handler, baton, pool);

  if (error) {
    char message[1024];
    svn_err_best_message(error, message, 1024);
    return Error(std::string(message));
  }

  Diff d(std::string(stringbuf->data, stringbuf->len));

  svn_pool_destroy(pool);

  return d;
}


inline Try<std::string> patch(const std::string& s, const Diff& diff)
{
  apr_initialize();

  apr_pool_t* pool = svn_pool_create(NULL);

  svn_string_t source;
  source.data = s.data();
  source.len = s.length();

  svn_txdelta_window_handler_t handler;
  void* baton = NULL;

  svn_stringbuf_t* stringbuf = svn_stringbuf_create_ensure(s.length(), pool);

  svn_txdelta_apply(
      svn_stream_from_string(&source, pool),
      svn_stream_from_stringbuf(stringbuf, pool),
      NULL,
      NULL,
      pool,
      &handler,
      &baton);

  svn_stream_t* stream = svn_txdelta_parse_svndiff(
      handler,
      baton,
      TRUE,
      pool);

  const char* data = diff.data.data();
  apr_size_t length = diff.data.length();

  svn_error_t* error = svn_stream_write(stream, data, &length);

  if (error) {
    char message[1024];
    svn_err_best_message(error, message, 1024);
    return Error(std::string(message));
  }

  std::string result(stringbuf->data, stringbuf->len);

  svn_pool_destroy(pool);  

  return result;
}

} // namespace svn {

#endif // __STOUT_SVN_HPP__
