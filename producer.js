// producer.js
const { Queue } = require('bullmq');

const myQueue = new Queue('test', {
  connection: { host: '127.0.0.1', port: 6379 }
});

// 添加任务
(async () => {
  const job = await myQueue.add('convert', {
    filename: 'user_upload.docx',
    jobId: 'xxx',
  });
  console.log('任务已添加:', job.id);
  await myQueue.close();
})();
