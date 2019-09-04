const http = require('http'),
	fs = require('fs'),
	CsvReadableStream = require('csv-reader'),
	moment = require('moment');
	url = 'http://www.anp.gov.br/images/infopreco/infopreco.csv';

const DataFile = {
	download: () => {
		return new Promise((resolve, reject) => {
			const path = `./data/${moment().format('YYYYMMDDHHmmss')}_data.csv`,
				file = fs.createWriteStream(path);
			
			file.on('open', () => {
				http.get(url,  res => {
					res.on('error', error => reject(error));

					res.pipe(file);
				});
			})
				.on('error', error => reject(error))
				.on('finish', () => resolve(path));
		});
	},

	extract: path => {
		return new Promise(resolve => {
			const data = [];
	
			fs.createReadStream(path, 'utf8')
				.pipe(CsvReadableStream({
					parseNumbers: true,
					parseBooleans: true,
					trim: true
				}))
				.on('data', row => {
					if (row.length < 3) {
						return false;
					}
					
					row[0] = row[0].split(';');
					row[1] = row[1].split(';');
					row[2] = row[2].split(';');
	
					data.push({
						cnpj: row[0][0],
						name: row[0][1],
						address: {
							street: row[0][2],
							number: row[1][0],
							more: row[1][1],
							region: row[1][2],
							city: row[1][3],
							uf: row[1][4]
						},
						fuel: row[1][5],
						price: Number(`${row[1][6]}.${row[2][0]}`),
						updatedAt: new Date(row[2][1])
					});
				})
				.on('end', () => resolve(data));
		});
	}
}

module.exports = DataFile;