from confluent_kafka import Producer
import sys
import time


# 用來接收從Consumer instance發出的error訊息
def error_cb(err):
    print('Error: %s' % err)

#def to_kafka():



# 主程式進入點
if __name__ == '__main__':
    # 步驟1. 設定要連線到Kafka集群的相關設定
    props = {
        # Kafka集群在那裡?
        'bootstrap.servers': '10.8.0.6:9092',          # <-- 置換成要連接的Kafka集群
        'error_cb': error_cb                            # 設定接收error訊息的callback函數
    }
    # 步驟2. 產生一個Kafka的Producer的實例
    producer = Producer(props)
    # 步驟3. 指定想要發佈訊息的topic名稱
    topicName = 'member'
    msgCount = 10000
    
    try:
        print('Start sending messages ...')
        # produce(topic, [value], [key], [partition], [on_delivery], [timestamp], [headers])
        for i in range(msgCount):
            producer.produce(topicName, key=str(i), value=str('userId:{}, movieId:{}'.format(i,i)))
            #producer.send(topicName, key=str(i), value=str(e04) )
            producer.poll(0)  # <-- (重要) 呼叫poll來讓client程式去檢查內部的Buffer
            print('key={}, value={}'.format(str(i), str(i)))
            time.sleep(3)  # 讓主執行緒停個3秒

        print('Send ' + str(msgCount) + ' messages to Kafka')
    except BufferError as e:
        # 錯誤處理
        sys.stderr.write('%% Local producer queue is full ({} messages awaiting delivery): try again\n'
                         .format(len(producer)))
    except Exception as e:
        print(e)
    # 步驟5. 確認所有在Buffer裡的訊息都己經送出去給Kafka了
    producer.flush(10)
    print('Message sending completed!')
