const mongoose = require('mongoose')
const Float = require('mongoose-float').loadType(mongoose)
const fs = require('fs')
const csv = require('fast-csv')

mongoose.connect('mongodb://localhost:27017/banco_dev', {
    useNewUrlParser: true,
    useUnifiedTopology: true
});

const sellModel = mongoose.model('sell', new mongoose.Schema({
    num_ped: String,
    client: String,
    seller: String,
    status: Number,
    dt_sell: Date,
    cat_prod: String,
    fam_prod: String,
    cod_prod: String,
    sell_value: Float,
    sell_quant: Float,
    oc: String
}))


async function run() {
    try {
        fs.createReadStream('PEDIDOS.CSV')
            .pipe(csv.parse({ headers: false }))
            .on('data', row => {
                console.log(row)
            })
            console.log('to aqui')
        /*
        await sellModel.create({
            num_ped:"201909100001",
            client:"0001-0582",
            seller:"101",
            status:"8",
            dt_sell:new Date(),
            cat_prod:"9",
            fam_prod:"255",
            cod_prod:"902580",
            sell_value:50.58,
            sell_quant:1.9,
            oc:"1"
        })
        console.log('await: ',await sellModel.find())
        */

    } catch (error) {
        console.log('deu error: ',error)
    }
}

run()