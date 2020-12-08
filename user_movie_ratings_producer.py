from confluent_kafka import Producer
import sys
import time


def produce(userId, movieId, rating):
     props = {
        # Kafka集群在那裡?
        'bootstrap.servers': '10.8.0.6:9092',          # <-- 置換成要連接的Kafka集群
        'error_cb': error_cb                            # 設定接收error訊息的callback函數
    }
    # 步驟2. 產生一個Kafka的Producer的實例
    producer = Producer(props)
    # 步驟3. 指定想要發佈訊息的topic名稱
    topicName = 'member'


    try:
        producer.produce(topicName, key=str(i), value=str('userId:{}, movieId:{}, rating:{}'.format(userId, movieId, rating))
        producer.poll(0) # <-- (重要) 呼叫poll來讓client程式去檢查內部的Buffer
    except BufferError as e:
        # 錯誤處理
        sys.stderr.write('%% Local producer queue is full ({} messages awaiting delivery): try again\n'
                         .format(len(producer)))
    except Exception as e:
        print(e)
    # 步驟5. 確認所有在Buffer裡的訊息都己經送出去給Kafka了
    producer.flush(10)
    print('Message sending completed!')

def error_cb(err):
    print('Error: %s' % err)



# if __name__ == '__main__':
#     main()