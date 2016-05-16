#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <ruby.h>

#include "librdkafka/rdkafka.h"

#define MAX_SHUTDOWN_TRIES 5

static rd_kafka_t *rk = NULL;

static void error(const char *fmt, ...)
{
  va_list args;

  va_start(args, fmt);
  vfprintf(stderr, fmt, args);
  va_end(args);
  fputs("\n", stderr);
}

static void logger(const rd_kafka_t *rk, int level,
                   const char *fac, const char *buf)
{
  fprintf(stderr, "CKafka [%d]: %s: %s\n", level, fac, buf);
}


static VALUE kafka_send(VALUE self, VALUE topic_value, VALUE key, VALUE message)
{
  rd_kafka_topic_conf_t *topic_conf = NULL;
  rd_kafka_topic_t *topic = NULL;
  char *topic_name = NULL;
  void *message_bytes = NULL;
  size_t message_len = 0;
  void *key_buf = NULL;
  size_t key_len = 0;
  int res = 0;

  if (!NIL_P(key)) {
    key_buf = RSTRING_PTR(key);
    key_len = RSTRING_LEN(key);
  }

  topic_name = StringValueCStr(topic_value);
  if (!topic_name) {
    rb_raise(rb_eStandardError, "topic is not a string!");
  }

  if(!NIL_P(message)) {
    message_bytes = RSTRING_PTR(message);
    if(!message_bytes) {
      rb_raise(rb_eStandardError, "failed to get message ptr");
    }
    message_len = RSTRING_LEN(message);
  }


  topic_conf = rd_kafka_topic_conf_new();
  if(!topic_conf) {
    rb_raise(rb_eStandardError, "failed to create kafka topic configuration");
  }

  topic = rd_kafka_topic_new(rk, topic_name, topic_conf);
  if(!topic) {
    rb_raise(rb_eStandardError, "failed to create topic");
  }

  res = rd_kafka_produce(topic, RD_KAFKA_PARTITION_UA, RD_KAFKA_MSG_F_COPY, message_bytes, message_len,
                         key_buf, key_len, NULL);

  if (res) {
    rb_raise(rb_eStandardError, "rd_kafka_produce failed: %d", res);
  }

  return Qnil;
}

static VALUE kafka_destroy()
{
  if(rk) {
    for(int i = 0 ; i < MAX_SHUTDOWN_TRIES; ++i) {
      if(!rd_kafka_outq_len(rk)) {
        break;
      }
      rd_kafka_poll(rk, 100);
    }

    rd_kafka_destroy(rk);
    int res = rd_kafka_wait_destroyed(100);
    if(res) {
      error("wait_destroyed returned: %d\n", res);
    }
    rk = NULL;
  }
  return Qnil;
}

static VALUE kafka_add_broker(VALUE self, VALUE broker)
{
  char *value = StringValueCStr(broker);
  int res;

  if(!value) {
    rb_raise(rb_eArgError, "invalid string");
  }

  res = rd_kafka_brokers_add(rk, value);
  if (res == 0) {
    rb_raise(rb_eStandardError, "failed to add any brokers!");
  }

  return Qnil;
}

#define LOG_DEBUG 7

static void error_cb(rd_kafka_t *rk, int err, const char *reason, void *opaque)
{
  error("[%d] %s\n", err, reason);
}

static VALUE kafka_init(VALUE self)
{
  rd_kafka_conf_t *conf;
  char errstr[512];

  if(rk) {
    kafka_destroy();
  }
  conf = rd_kafka_conf_new();

  rd_kafka_conf_set_error_cb(conf, error_cb);
  rd_kafka_conf_set_log_cb(conf, logger);

  rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr));
  if (!rk) {
    rb_raise(rb_eStandardError, "failed to create kafka producer: %s\n", errstr);
  }

  return Qnil;
}

VALUE Init_ckafka()
{

  VALUE kafka_module = rb_define_module("Ckafka");

  rb_define_singleton_method(kafka_module, "init", kafka_init, 0);
  rb_define_singleton_method(kafka_module, "produce", kafka_send, 3);
  rb_define_singleton_method(kafka_module, "add_broker", kafka_add_broker, 1);
  rb_define_singleton_method(kafka_module, "close", kafka_destroy, 0);

  kafka_init(Qnil);

  return Qnil;
}
