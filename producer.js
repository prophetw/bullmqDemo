// producer.js
const { Queue } = require('bullmq');

const myQueue = new Queue('test', {
  connection: { host: '127.0.0.1', port: 6379 }
});

// 添加任务
(async () => {
  try {
    const job = await myQueue.add('convert', {
      filename: 'user_upload.docx',
      jobId: 'xxx',
    });
    console.log('任务已添加:', job.id);
  } catch (error) {
    console.error('添加任务失败:', error.message);
  } finally {
    await myQueue.close();
  }
})();
