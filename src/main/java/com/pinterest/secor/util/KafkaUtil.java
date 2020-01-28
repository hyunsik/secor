package com.pinterest.secor.util;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.TopicPartition;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

public class KafkaUtil {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaUtil.class);

  public static <K, V> KafkaConsumer<K, V> newKafkaConsumer(SecorConfig config,
                                                            Deserializer<K> keyDeserializer,
                                                            Deserializer<V> valueDeserializer)
      throws UnknownHostException {
    if (!config.getKafkaTopicBlacklist().isEmpty() && !config.getKafkaTopicFilter().isEmpty()) {
      throw new RuntimeException("Topic filter and blacklist cannot be both specified.");
    }
    final KafkaConsumer<K, V> consumer = new KafkaConsumer(createProperties(config), keyDeserializer, valueDeserializer);
    selectedTopics(consumer, config);
    return consumer;
  }

  protected static Properties createProperties(SecorConfig mConfig) throws UnknownHostException {
    final Properties props = new Properties();
    props.put("bootstrap.servers", mConfig.getKafkaSeedBrokerHost());
    props.put("group.id", mConfig.getKafkaGroup());

    props.put("enable.auto.commit", "false");
    props.put("auto.commit.enable", "false");
    props.put("auto.offset.reset", mConfig.getConsumerAutoOffsetReset());
    props.put("consumer.timeout.ms", Integer.toString(mConfig.getConsumerTimeoutMs()));
    props.put("consumer.id", IdUtil.getConsumerId());

    props.put("partition.assignment.strategy", mConfig.getPartitionAssignmentStrategy());
    if (mConfig.getRebalanceMaxRetries() != null &&
        !mConfig.getRebalanceMaxRetries().isEmpty()) {
      props.put("rebalance.max.retries", mConfig.getRebalanceMaxRetries());
    }
    if (mConfig.getRebalanceBackoffMs() != null &&
        !mConfig.getRebalanceBackoffMs().isEmpty()) {
      props.put("rebalance.backoff.ms", mConfig.getRebalanceBackoffMs());
    }
    if (mConfig.getSocketReceiveBufferBytes() != null &&
        !mConfig.getSocketReceiveBufferBytes().isEmpty()) {
      props.put("socket.receive.buffer.bytes", mConfig.getSocketReceiveBufferBytes());
    }
    if (mConfig.getFetchMessageMaxBytes() != null && !mConfig.getFetchMessageMaxBytes().isEmpty()) {
      props.put("fetch.message.max.bytes", mConfig.getFetchMessageMaxBytes());
    }
    if (mConfig.getFetchMinBytes() != null && !mConfig.getFetchMinBytes().isEmpty()) {
      props.put("fetch.min.bytes", mConfig.getFetchMinBytes());
    }
    if (mConfig.getSessionTimeout() != null && !mConfig.getSessionTimeout().isEmpty()) {
      props.put("session.timeout.ms", mConfig.getSessionTimeout());
    }
    if (mConfig.getMaxPollRecords() != null && !mConfig.getMaxPollRecords().isEmpty()) {
      props.put("max.poll.records", mConfig.getMaxPollRecords());
    }
    if (mConfig.getMaxPollInterval() != null && !mConfig.getMaxPollInterval().isEmpty()) {
      props.put("max.poll.interval.ms", mConfig.getMaxPollInterval());
    }
    if (mConfig.getHeartbeatInterval() != null && !mConfig.getHeartbeatInterval().isEmpty()) {
      props.put("heartbeat.interval.ms", mConfig.getHeartbeatInterval());
    }
    if (mConfig.getRequestTimeoutMs() != null && !mConfig.getRequestTimeoutMs().isEmpty()) {
      props.put("request.timeout.ms", mConfig.getRequestTimeoutMs());
    }
    if (mConfig.getFetchMaxWaitMs() != null && !mConfig.getFetchMaxWaitMs().isEmpty()) {
      props.put("fetch.max.wait.ms", mConfig.getFetchMaxWaitMs());
    }
    if (mConfig.getMaxPartitionFetchBytes() != null && !mConfig.getMaxPartitionFetchBytes().isEmpty()) {
      props.put("max.partition.fetch.bytes", mConfig.getMaxPartitionFetchBytes());
    }

    return props;
  }

  private static void selectedTopics(KafkaConsumer<?, ?> consumer, SecorConfig mConfig) {
    Set<String> allTopics = consumer.listTopics().keySet();
    final Set<String> selected = new HashSet<String>();
    if (mConfig.getKafkaTopicBlacklist().isEmpty()) {
      final Pattern pattern = Pattern.compile(mConfig.getKafkaTopicFilter());
      for (String topic : allTopics) {
        if (pattern.matcher(topic).matches()) {
          selected.add(topic);
        }
      }
    } else {
      final Pattern pattern = Pattern.compile(mConfig.getKafkaTopicBlacklist());
      for (String topic : allTopics) {
        if (!pattern.matcher(topic).matches()) {
          selected.add(topic);
        }
      }
    }

    LOG.info("Subscribe {} Topics: {}", selected.size(), StringUtils.join(selected.iterator(), ", "));
    consumer.subscribe(selected);
  }

  public static final org.apache.kafka.common.TopicPartition convertTopicPartition(TopicPartition topicPartition) {
    return new org.apache.kafka.common.TopicPartition(topicPartition.getTopic(), topicPartition.getPartition());
  }
}
